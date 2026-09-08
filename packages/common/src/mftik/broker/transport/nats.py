"""The NATS transport — subjects, streams, consumers and KV buckets.

Every family the broker speaks maps onto something NATS already has, and the
mapping is worth stating in one place because three of the choices are not the
obvious one.

**Fan-out is JetStream, and every subscriber is a consumer of its own.** Core
NATS would deliver a published message to whoever is listening and forget it,
which is what Redis Pub/Sub does and would have been the smaller change. It is
JetStream here so a subject has a history: the same stream that carries a feed
to a live session is the thing a later reader can address by offset. Each
:meth:`NatsTransport.subscribe` creates an ephemeral consumer starting at *new*,
which is what reproduces the promise the broker makes — a message published
while nobody was subscribed is gone — while leaving the stored message there for
anything that wants to read the subject rather than follow it.

**Request-reply is core NATS, except :meth:`NatsTransport.post`.** A caller
waiting on an answer gains nothing from durability: it has a timeout, and a
request executed after that timeout passed is a side effect nobody is expecting
any more. Core request-reply also answers *better* — no responders is an
immediate error rather than five seconds of silence, so the control plane learns
that a plane is down in milliseconds. ``post`` is the one caller with no
timeout to fall back on, so it alone rides a work-queue stream; see the method.

**Not everything Redis kept in one keyspace belongs in KV.** State, leases and
counters do — they are current values addressed by name, which is what a bucket
is. The log tail and the tape do not: they are histories with a retention
policy, which is what a stream is. Splitting them that way is what lets each
carry its own age, and it is why the tape gets a stream per feed rather than
one stream for all of them.

Two things the Redis transport could do and this one cannot, both from the same
place. NATS' per-message TTL is whole seconds with a one second floor
(ADR-43), so a sub-second lease rounds up to one second — production leases are
thirty. And a lease name cannot be spelled with ``:``, because a KV key is
restricted to ``[-/_=.a-zA-Z0-9]``, so the colons the Redis key shapes use
become dots on the way in.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import re
from collections.abc import AsyncIterator, Mapping, Sequence

import nats
import nats.errors
import nats.js.api as js_api
import nats.js.errors
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

from mftik.broker.config import BrokerConfig
from mftik.broker.errors import BrokerNotConnectedError, RequestTimeoutError
from mftik.broker.transport.base import (
    LEASE_ANONYMOUS,
    BrokerTransport,
    redacted_url,
)

logger = logging.getLogger(__name__)

#: The shortest lease NATS can express. ADR-43: a ``Nats-TTL`` below one second
#: — a literal ``0`` included — is rejected and the message discarded, so a
#: shorter request is rounded up rather than sent and lost.
MIN_TTL_SECONDS = 1

#: How long the fan-out stream keeps a subject's messages. Matches
#: ``publish_log``'s default TTL, because that is the longest any caller asks a
#: fan-out topic to remember anything for.
FANOUT_MAX_AGE_SECONDS = 86_400

#: How many messages one fan-out subject keeps. Every pub/sub subject gets a
#: tail, not only the log topics, and that is deliberate: ``publish_log`` and
#: ``publish`` then differ only in whether the caller intends to read it back,
#: rather than in where the message went. It also bounds the stream by the
#: number of live subjects instead of by traffic — a busy feed cannot grow it.
#:
#: It is therefore a *ceiling* on what :meth:`NatsTransport.publish_log` can be
#: asked to keep, and has to stay above every caller's ask. It was 100, which is
#: ``BrokerConfig.log_buffer_maxlen``'s default and looked like the same number —
#: but STS and the API both ask for 200 for the session status ring, and a
#: request above the ceiling is served silently halved. Above it now raises, and
#: this is sized well clear of both.
FANOUT_MAX_MSGS_PER_SUBJECT = 256

#: Global fuse on the fan-out stream, in messages. Reached only if subjects
#: themselves multiply without end, so it is what a session-churn bug hits
#: instead of the disk.
FANOUT_MAX_MSGS = 1_000_000

#: How long to wait before re-asking a subject that reported no responders, and
#: how many times. Three attempts and 50ms is ~100ms spent before calling a
#: subject unserved — long enough to cover a serve loop registering as its
#: process boots, and still fifty times inside the five second timeout that is
#: the alternative answer.
_NO_RESPONDERS_GRACE_S = 0.05
_NO_RESPONDERS_ATTEMPTS = 3

#: How long a read's fetch waits before giving up. Sized against a server one
#: round trip away and never reached in the normal case, because every read
#: below knows how many messages it is asking for.
_READ_TIMEOUT_S = 2.0

#: How many messages one fetch asks for at most.
_READ_BATCH = 256

#: How long a read's consumer survives if this process dies mid-read. Short: it
#: exists for the length of one call.
_READ_CONSUMER_IDLE_S = 30.0

#: How long one pull for posted work parks before looking at its stop event.
#: Cancellable, unlike Redis' blocking pop, so this bounds nothing a caller
#: waits on — it only decides how often an idle serve loop wakes.
_POST_FETCH_TIMEOUT_S = 1.0

#: Where a tape record carries the stamp its writer chose, when it chose one.
#: A header rather than a field on the record, so it cannot collide with
#: anything a feed prints.
RECORDED_MS_HEADER = "Mftik-Recorded-Ms"

#: How long :meth:`NatsTransport.close` waits for the outbound buffer. Short:
#: everything still in it has already been published to a server that is one
#: round trip away, and a shutdown that hangs is worse than one that gives up.
_CLOSE_FLUSH_TIMEOUT_S = 2.0

#: What a KV key may contain. NATS' own rule, restated here so a bad name is
#: refused with a message naming the value rather than deep inside the client.
_KV_KEY_OK = re.compile(r"^[-/_=.a-zA-Z0-9]+$")

#: What one subject token may not contain. ``.`` is absent on purpose: the
#: broker's topics are already dotted and those dots are meant as hierarchy.
_SUBJECT_BAD = re.compile(r"[\s*>]")


def _ttl_seconds(ttl: float) -> int:
    """A lease TTL as whole seconds, never below the floor.

    Rounds up rather than down. A lease is a claim on a resource, and the
    failure mode of one that expired early is two processes holding the same
    account, while the cost of one that expired a fraction late is a redeploy
    waiting slightly longer.
    """
    return max(MIN_TTL_SECONDS, int(-(-ttl // 1)))


def _sanitize(name: str) -> str:
    """A stream or consumer name out of a subject or a feed key.

    NATS forbids ``.``, ``*``, ``>``, whitespace and path separators in these,
    so the dots that make a subject readable have to go. Kept as a
    transliteration rather than a hash because these names are what an operator
    reads out of ``nats stream ls`` when they are trying to find out what a node
    is doing, and ``mft_tape_aggtrade_Gate_Spot_ETHUSDT`` answers that where a
    digest does not.

    The transliteration is not injective in general — ``a.b`` and ``a_b`` both
    arrive at ``a_b`` — which is why :meth:`NatsTransport._named` refuses a
    second original that lands on a name already taken. In practice nothing
    collides: feeds are ``{topic}.{UniversalTicker}`` off a fixed vocabulary and
    a validated ticker, and RPC subjects are built by
    :class:`~mftik.protocol.Topics`.
    """
    return re.sub(r"[^-a-zA-Z0-9]", "_", name)


def _kv_key(name: str) -> str:
    """A KV key out of a broker name.

    The names were Redis key tails and are spelled with colons —
    ``sts:alive:{session}``, ``backfill:lock:{api_id}``, ``cid:slot``. A KV key
    may not contain one, so they become dots, which NATS reads as hierarchy and
    which is what the segments always were.

    Anything still outside the allowed set raises here rather than at the
    client, because the values that could get this far come from outside the
    node — a venue's symbol, a credential's api_key — and an error naming the
    value is worth far more than one naming the header it failed in.
    """
    key = name.replace(":", ".")
    if not _KV_KEY_OK.match(key):
        raise ValueError(
            f"{name!r} cannot be a NATS KV key: only letters, digits and "
            f"- / _ = . are allowed (: is mapped to . for you)"
        )
    return key


def _check_subject(topic: str) -> str:
    """Refuse a topic that would not survive being a subject.

    Redis channels accept any bytes, so a topic built out of a credential or a
    venue's symbol has never had to be checked. A NATS subject is parsed: a
    space or a stray wildcard in one segment changes what the message means or
    stops it being deliverable at all, and it would surface at the publish
    rather than where the bad value came from.
    """
    if not topic or _SUBJECT_BAD.search(topic) or ".." in topic:
        raise ValueError(
            f"{topic!r} cannot be a NATS subject: no whitespace, '*', '>' or "
            f"empty segments"
        )
    return topic


class NatsTransport(BrokerTransport):
    """A NATS connection, its JetStream context, and the names this node owns."""

    def __init__(
        self,
        config: BrokerConfig,
        *,
        connection: NatsClient | None = None,
    ) -> None:
        self.config = config
        self._nc = connection
        self._owns_connection = connection is None
        self._js: nats.js.JetStreamContext | None = None
        self._kv: dict[str, nats.js.kv.KeyValue] = {}
        # Streams and consumers this process has already made sure of. A
        # transport that re-checked on every append would pay an API round trip
        # per market-data print.
        self._ensured: set[str] = set()
        self._names: dict[str, str] = {}
        self._lock = asyncio.Lock()
        # One per state name. See :meth:`_state_lock`. Bounded by how many
        # names this process writes — an account's book and its ledger — not by
        # traffic.
        self._state_locks: dict[str, asyncio.Lock] = {}

    # --- names -------------------------------------------------------------

    @property
    def _prefix(self) -> str:
        return self.config.key_prefix

    def _fanout_subject(self, topic: str) -> str:
        return f"{self._prefix}.ps.{_check_subject(topic)}"

    def _fanout_topic(self, subject: str) -> str:
        """The topic a caller asked for, back out of the subject it arrived on."""
        return subject[len(self._prefix) + 4 :]

    def _rpc_subject(self, subject: str) -> str:
        """Where a live request is asked. Core NATS, and no stream over it.

        Separate from :meth:`_post_subject`, and it has to be. A JetStream
        stream is a subscriber like any other, so a stream whose filter covered
        this subject would receive every core request — and answer it, on the
        requester's own reply subject, with a publish acknowledgement. The
        caller then parses ``{"stream": ..., "seq": 1}`` as the reply it was
        waiting for, and the handler's real answer arrives second to an inbox
        nobody is reading any more.
        """
        return f"{self._prefix}.rpc.{_check_subject(subject)}"

    def _post_subject(self, subject: str) -> str:
        """Where durable work is left. Captured by the work-queue stream."""
        return f"{self._prefix}.post.{_check_subject(subject)}"

    def _tape_subject(self, feed: str) -> str:
        return f"{self._prefix}.tape.{_check_subject(feed)}"

    @property
    def _fanout_stream(self) -> str:
        return _sanitize(f"{self._prefix}_ps")

    @property
    def _post_stream(self) -> str:
        return _sanitize(f"{self._prefix}_post")

    def _named(self, kind: str, original: str) -> str:
        """A stream or consumer name for ``original``, unique within this node.

        Guards :func:`_sanitize`'s one weakness. Two different originals that
        transliterate to the same name would share a stream, which for the tape
        means two feeds' records interleaved in one history and a warm-up
        reading somebody else's prints. Nothing in the current naming can do
        that; this is what makes it stay true.
        """
        name = _sanitize(f"{self._prefix}_{kind}_{original}")
        taken = self._names.setdefault(name, original)
        if taken != original:
            raise ValueError(
                f"{original!r} and {taken!r} both name the NATS {kind} "
                f"{name!r}; one of them has to be spelled differently"
            )
        return name

    # --- lifecycle ---------------------------------------------------------

    @property
    def js(self) -> nats.js.JetStreamContext:
        if self._js is None:
            raise BrokerNotConnectedError(
                "Broker is not connected; call connect() first"
            )
        return self._js

    @property
    def nc(self) -> NatsClient:
        if self._nc is None or self._js is None:
            raise BrokerNotConnectedError(
                "Broker is not connected; call connect() first"
            )
        return self._nc

    async def connect(self) -> None:
        if self._nc is None:
            self._nc = await nats.connect(
                self.config.nats_url,
                # Reconnect for as long as the process lives. The alternative
                # is a plane that gives up on the bus and stays up not doing
                # anything, which is the failure the Redis transport's retry
                # policy exists to avoid as well.
                max_reconnect_attempts=-1,
                # Everything published while the connection is down is held and
                # flushed on reconnect. That is right for fan-out and for
                # posted work, and harmless for a request, which has its own
                # deadline and will fail on that instead.
                pending_size=8 * 1024 * 1024,
            )
            self._owns_connection = True
        self._js = self._nc.jetstream()
        await self._ensure_fanout_stream()
        await self._ensure_post_stream()

    async def close(self) -> None:
        if self._nc is not None and self._owns_connection:
            # Flush, then close. Not ``drain()``: draining waits for every
            # subscription's handler to finish, and a process shutting down has
            # exactly the subscriptions whose loops are already being cancelled
            # — so it reliably waits out its own timeout and turns a teardown
            # into thirty seconds. The flush is the part worth keeping, because
            # posted work still sitting in the outbound buffer is work that was
            # accepted and then lost.
            with contextlib.suppress(Exception):
                await self._nc.flush(timeout=_CLOSE_FLUSH_TIMEOUT_S)
            with contextlib.suppress(Exception):
                await self._nc.close()
            self._nc = None
        self._js = None
        self._kv.clear()
        self._ensured.clear()

    def describe(self) -> str:
        """Which server this ended up on, with the credential taken out.

        The connected URL rather than the configured one, because ``NATS_URL``
        may name several and which one a plane is actually on is the thing worth
        having in a log. Both go through :func:`redacted_url`: ``connected_url``
        is the URL as given, userinfo included, so reading ``.netloc`` off it
        prints the password — and this line is logged by every plane on every
        boot.
        """
        connected = getattr(self._nc, "connected_url", None)
        url = self.config.nats_url if connected is None else connected.geturl()
        return f"NATS at {redacted_url(url)}"

    # --- streams and buckets -----------------------------------------------

    async def _ensure_fanout_stream(self) -> None:
        """One stream for every pub/sub subject this node publishes.

        A catch-all ``{prefix}.ps.>`` rather than a stream per subject family,
        because a family this did not list would fail at the publish — and the
        families are added by whoever writes a new topic, who has no reason to
        come here. The per-subject bounds are what keep one stream honest: each
        subject holds its last :data:`FANOUT_MAX_MSGS_PER_SUBJECT` messages, so
        the size follows how many subjects are live rather than how fast the
        busiest one prints.
        """
        await self._ensure_stream(
            js_api.StreamConfig(
                name=self._fanout_stream,
                subjects=[f"{self._prefix}.ps.>"],
                retention=js_api.RetentionPolicy.LIMITS,
                discard=js_api.DiscardPolicy.OLD,
                max_msgs_per_subject=FANOUT_MAX_MSGS_PER_SUBJECT,
                max_msgs=FANOUT_MAX_MSGS,
                max_age=FANOUT_MAX_AGE_SECONDS,
                # What lets ``publish_log`` honour the TTL it is handed. Without
                # it the header is refused and the line is dropped, so this and
                # that header have to be changed together.
                allow_msg_ttl=True,
                allow_direct=True,
            )
        )

    async def _ensure_post_stream(self) -> None:
        """The work queue behind :meth:`post`, and nothing else.

        Its own subject space, not shared with ``request`` and ``probe`` — see
        :meth:`_rpc_subject` for what happens when a stream can see a core
        request. What lands here is work whose sender has already moved on,
        which is why the retention is ``workqueue``: a message lives until some
        consumer acknowledges it, and one nobody is serving waits instead of
        expiring.
        """
        await self._ensure_stream(
            js_api.StreamConfig(
                name=self._post_stream,
                subjects=[f"{self._prefix}.post.>"],
                retention=js_api.RetentionPolicy.WORK_QUEUE,
                discard=js_api.DiscardPolicy.OLD,
            )
        )

    async def _ensure_stream(self, config: js_api.StreamConfig) -> None:
        assert config.name is not None
        if config.name in self._ensured:
            return
        async with self._lock:
            if config.name in self._ensured:
                return
            try:
                await self.js.add_stream(config)
            except nats.js.errors.BadRequestError:
                # Already there with a different shape — an older node's, or
                # this node's before a constant here changed. Updating is right:
                # the config in this file is what the code assumes, and a stream
                # that disagrees is what silently breaks a retention promise.
                await self.js.update_stream(config)
            self._ensured.add(config.name)

    async def _bucket(self, kind: str) -> nats.js.kv.KeyValue:
        """The KV bucket for ``kind``, created on first use.

        One bucket per family rather than one for the node, so ``state_all``
        can read a prefix without meeting leases on the way, and so the three
        can be given different history and TTL later without moving keys.
        """
        existing = self._kv.get(kind)
        if existing is not None:
            return existing
        async with self._lock:
            existing = self._kv.get(kind)
            if existing is not None:
                return existing
            name = _sanitize(f"{self._prefix}_{kind}")
            bucket = await self.js.create_key_value(
                js_api.KeyValueConfig(bucket=name, history=1)
            )
            self._kv[kind] = bucket
            return bucket

    # --- fan-out -----------------------------------------------------------

    async def publish(self, topic: str, raw: str) -> None:
        await self.js.publish(self._fanout_subject(topic), raw.encode())

    async def subscribe(
        self, topics: Sequence[str], *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str]]:
        async for item in self._consume(
            [self._fanout_subject(t) for t in topics], stop=stop
        ):
            yield item

    async def psubscribe(
        self, patterns: Sequence[str], *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str]]:
        # Patterns are subjects with wildcards in them, so they pass through
        # ``_check_subject``'s refusal of ``*`` — prefixed by hand instead.
        async for item in self._consume(
            [f"{self._prefix}.ps.{p}" for p in patterns], stop=stop
        ):
            yield item

    async def _consume(
        self, subjects: Sequence[str], *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str]]:
        """One ephemeral consumer per subject, merged, until ``stop``.

        A consumer each is what the fan-out pattern is: the stream holds one
        copy of the message and every subscriber reads it at its own position,
        so a slow reader cannot cost a fast one anything and neither can see the
        other's. Starting at *new* and acknowledging nothing is what makes it
        behave like the broadcast the broker promises rather than like a queue.
        """
        if not subjects:
            raise ValueError("a subscription needs at least one subject")

        inbound: asyncio.Queue[tuple[str, str]] = asyncio.Queue()

        async def handler(msg: Msg) -> None:
            await inbound.put((self._fanout_topic(msg.subject), msg.data.decode()))

        subs = [
            await self.js.subscribe(
                subject,
                cb=handler,
                config=js_api.ConsumerConfig(
                    deliver_policy=js_api.DeliverPolicy.NEW,
                    ack_policy=js_api.AckPolicy.NONE,
                    inactive_threshold=self.config.consumer_idle_seconds,
                ),
            )
            for subject in subjects
        ]
        try:
            async for item in _iter_until_stopped(inbound, stop=stop):
                yield item
        finally:
            for sub in subs:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()

    # --- fan-out with a tail -----------------------------------------------

    async def publish_log(
        self, topic: str, raw: str, *, maxlen: int, ttl_seconds: int
    ) -> None:
        """Publish, hold this subject to ``maxlen``, and expire the line.

        Two round trips rather than one, and that is the honest cost of an exact
        ring here: a purge keeping the newest ``maxlen`` is the only per-subject
        bound JetStream will take, and it cannot be pipelined behind the publish
        the way Redis pipelines its ``LTRIM``. Only ``publish_log`` pays it;
        plain :meth:`publish` does not, which is now the real difference between
        the two.

        ``maxlen`` above :data:`FANOUT_MAX_MSGS_PER_SUBJECT` raises. The stream
        has already discarded by then, so there is nothing a purge could recover
        and a caller quietly given half the ring it asked for is worse than one
        told it asked for too much.

        ``ttl_seconds`` is a per-message TTL, which is not quite what Redis does
        with it — see the contract note on
        :meth:`~mftik.broker.transport.base.BrokerTransport.publish_log`.
        """
        if maxlen > FANOUT_MAX_MSGS_PER_SUBJECT:
            raise ValueError(
                f"publish_log(maxlen={maxlen}) is above this transport's "
                f"per-subject ceiling of {FANOUT_MAX_MSGS_PER_SUBJECT}; raise "
                f"FANOUT_MAX_MSGS_PER_SUBJECT if a ring that long is wanted"
            )
        subject = self._fanout_subject(topic)
        await self.js.publish(
            subject,
            raw.encode(),
            headers={js_api.Header.MSG_TTL: str(_ttl_seconds(ttl_seconds))},
        )
        if maxlen < FANOUT_MAX_MSGS_PER_SUBJECT:
            await self.js.purge_stream(
                self._fanout_stream, subject=subject, keep=maxlen
            )

    async def fetch_log_buffer(self, topic: str) -> list[str]:
        """Everything the fan-out stream is still holding for ``topic``.

        All of it rather than the newest N, and the cap is what makes that safe:
        a subject holds at most :data:`FANOUT_MAX_MSGS_PER_SUBJECT` messages, so
        "all of them" is bounded by config rather than by traffic.
        """
        subject = self._fanout_subject(topic)
        held = (await self._subject_counts(self._fanout_stream, subject)).get(
            subject, 0
        )
        if not held:
            return []
        rows = await self._read(
            self._fanout_stream,
            subject,
            expected=held,
            config=js_api.ConsumerConfig(
                deliver_policy=js_api.DeliverPolicy.ALL,
                ack_policy=js_api.AckPolicy.NONE,
                filter_subject=subject,
                inactive_threshold=_READ_CONSUMER_IDLE_S,
            ),
        )
        return [raw for _ms, raw in rows]

    async def _subject_counts(self, stream: str, pattern: str) -> dict[str, int]:
        """How many messages each live subject under ``pattern`` is holding.

        The cheap half of every read here, and the reason none of them guess. A
        consumer cannot say how much it is about to deliver, so a fetch without
        an expected count has to wait out its own timeout to learn it has them
        all — which, on the paths a strategy reads its ledger through, was a
        whole second per call.
        """
        try:
            info = await self.js.stream_info(stream, subjects_filter=pattern)
        except nats.js.errors.NotFoundError:
            return {}
        return dict(info.state.subjects or {})

    async def _read(
        self,
        stream: str,
        subject: str,
        *,
        expected: int,
        config: js_api.ConsumerConfig,
    ) -> list[tuple[int, str]]:
        """``expected`` messages through a consumer of ``config``, oldest first.

        ``expected`` is what makes this prompt: the batch is sized to it, so the
        server answers as soon as it has that many rather than when a timeout
        expires. It is an upper bound, not a promise — a fetch that comes up
        short returns what it got.
        """
        if expected <= 0:
            return []
        sub = await self.js.pull_subscribe(subject, stream=stream, config=config)
        rows: list[tuple[int, str]] = []
        try:
            while len(rows) < expected:
                try:
                    msgs = await sub.fetch(
                        batch=min(expected - len(rows), _READ_BATCH),
                        timeout=_READ_TIMEOUT_S,
                    )
                except (nats.errors.TimeoutError, TimeoutError):
                    break
                if not msgs:
                    break
                for msg in msgs:
                    rows.append((_stamp_ms(msg), msg.data.decode()))
        finally:
            with contextlib.suppress(Exception):
                await sub.unsubscribe()
        return rows

    # --- request-reply -----------------------------------------------------

    def reply_inbox(self, request_id: str) -> str | None:
        """``None``: NATS carries a reply subject of its own beside the message.

        Redis has to put the address in the envelope because the serving process
        sees nothing else. Here the address is created by the request itself and
        handed to the server by the protocol, so there is nothing for the broker
        to stamp on the way out — :meth:`serve` produces it on the way in.
        """
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
        """Ask, and give a subject a moment to have somebody on it.

        No responders is the server saying its subscription table had nobody on
        this subject *at the instant the message arrived*, which is nearly always
        "that plane is down" and occasionally "that plane is coming up". Treating
        one sample as final is a race with every boot: a plane whose serve loop
        registers a millisecond after the API's first request would be reported
        down, and on the anycast subjects the whole pool can look empty during a
        rolling restart.

        So it is re-asked a couple of times, briefly, and only within the
        caller's own deadline. A subject that is really unserved still fails in
        a fraction of a second rather than the whole timeout, which is what makes
        core request-reply worth having here — it is the difference between the
        control plane learning a plane is down now and learning it five seconds
        from now, and the grace below is two orders of magnitude inside that.
        """
        deadline = asyncio.get_running_loop().time() + timeout
        subject_name = self._rpc_subject(subject)
        payload = raw.encode()
        for attempt in range(_NO_RESPONDERS_ATTEMPTS):
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                msg = await self.nc.request(
                    subject_name, payload, timeout=remaining
                )
            except nats.errors.NoRespondersError:
                last = attempt == _NO_RESPONDERS_ATTEMPTS - 1
                grace = _NO_RESPONDERS_GRACE_S
                if last or deadline - asyncio.get_running_loop().time() <= grace:
                    break
                await asyncio.sleep(grace)
                continue
            except (nats.errors.TimeoutError, TimeoutError):
                break
            return msg.data.decode()
        raise RequestTimeoutError(subject, request_id, timeout) from None

    async def probe(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        inbox: str | None,
        timeout: float,
    ) -> str:
        """Exactly :meth:`request`, and that is the point.

        The Redis transport needs a capped, expiring queue here to stop a
        dashboard's probes accumulating against a down instance. Core NATS
        stores nothing anywhere, so there is no queue to cap: a probe nobody
        answers has already left no trace by the time the caller gives up.
        """
        return await self.request(
            subject, raw, request_id=request_id, inbox=inbox, timeout=timeout
        )

    async def post(self, subject: str, raw: str) -> None:
        await self.js.publish(self._post_subject(subject), raw.encode())

    async def serve(
        self, subject: str, *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str | None]]:
        """Both halves of one subject: live requests and posted work.

        Two sources, because :meth:`request` and :meth:`post` reach a subject by
        different routes and a handler must not have to know which. Live
        requests arrive on a core queue subscription, where the queue group is
        what makes several processes of a plane a pool rather than all of them
        answering. Posted work arrives from the work-queue stream through a
        durable shared by name, which is the same sharing for the same reason.

        Neither source may end this loop. A fetch that timed out is an idle
        subject, and a connection that dropped is being reconnected underneath —
        both mean go round again, because a plane whose control loop returned
        stays up with nobody reading its requests.
        """
        live_subject = self._rpc_subject(subject)
        group = self._named("group", subject)
        inbound: asyncio.Queue[tuple[str, str | None]] = asyncio.Queue()

        async def handler(msg: Msg) -> None:
            await inbound.put((msg.data.decode(), msg.reply or None))

        live = await self.nc.subscribe(live_subject, queue=group, cb=handler)
        posted = asyncio.create_task(self._pump_posted(subject, inbound, stop=stop))
        try:
            async for item in _iter_until_stopped(inbound, stop=stop):
                yield item
        finally:
            posted.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await posted
            with contextlib.suppress(Exception):
                await live.unsubscribe()

    async def _pump_posted(
        self,
        subject: str,
        inbound: asyncio.Queue[tuple[str, str | None]],
        *,
        stop: asyncio.Event | None,
    ) -> None:
        """Hand posted work to the serve loop, acknowledging on delivery.

        Acknowledged as it is handed over rather than after the handler is done,
        which is what the Redis transport does too — a blocking pop takes the
        element off the list before any handler sees it. So the durability this
        buys is "nobody was serving the subject yet", which is what ``post``'s
        callers need, and not "the process died half way through the work",
        which neither transport has ever offered.
        """
        post_subject = self._post_subject(subject)
        durable = self._named("post", subject)
        sub = None
        while stop is None or not stop.is_set():
            try:
                if sub is None:
                    sub = await self.js.pull_subscribe(
                        post_subject,
                        durable=durable,
                        stream=self._post_stream,
                        config=js_api.ConsumerConfig(
                            durable_name=durable,
                            ack_policy=js_api.AckPolicy.EXPLICIT,
                            filter_subject=post_subject,
                            inactive_threshold=self.config.consumer_idle_seconds,
                        ),
                    )
                msgs = await sub.fetch(batch=16, timeout=_POST_FETCH_TIMEOUT_S)
            except (nats.errors.TimeoutError, TimeoutError):
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                # A consumer the server reaped, a reconnect, a stream that is
                # briefly not there. Rebuild it next time round rather than
                # ending the loop and taking the subject's control plane down.
                logger.warning(
                    "broker posted-work pull failed subject=%s — retrying",
                    subject,
                    exc_info=True,
                )
                sub = None
                await asyncio.sleep(_POST_FETCH_TIMEOUT_S)
                continue
            for msg in msgs:
                with contextlib.suppress(Exception):
                    await msg.ack()
                # No reply address: this is work whose sender is already gone.
                await inbound.put((msg.data.decode(), None))

    async def send_reply(self, inbox: str, raw: str) -> None:
        await self.nc.publish(inbox, raw.encode())

    # --- shared state ------------------------------------------------------

    def _state_key(self, name: str, field: str) -> str:
        return _kv_key(f"{name}.{field}")

    def _state_lock(self, name: str) -> asyncio.Lock:
        """Serialise this process' writes to one state name.

        Redis gets this from the server: a ``MULTI`` is one round trip, so two
        writers' commands cannot interleave and the last one issued is the one
        that stands. KV has no transaction across keys, so
        :meth:`state_replace` is three round trips and two of them *can*
        interleave — and the callers doing so are not hypothetical. TD writes
        an order and, from the OMS callback that same write triggered, schedules
        a whole-book replace as its own task; a handful of those are in flight
        at once during a burst of fills. Without a lock the one that finishes
        last wins rather than the one that started last, which puts a cancelled
        order back in the book.

        A lock inside the process is enough because a state name has exactly one
        writer — the session that owns that account — so there is no second
        process to race. Ordering, not mutual exclusion, is what this buys:
        asyncio runs ready tasks in the order they were created, so acquiring
        here in that order is what makes "last issued wins" true again.
        """
        lock = self._state_locks.get(name)
        if lock is None:
            lock = self._state_locks[name] = asyncio.Lock()
        return lock

    async def state_put_many(self, name: str, values: Mapping[str, str]) -> None:
        if not values:
            return
        async with self._state_lock(name):
            await self._put_fields(name, values)

    async def _put_fields(self, name: str, values: Mapping[str, str]) -> None:
        """Every field of one write, in flight together.

        A field is a key of its own here, so a mapping is several writes where
        Redis has one ``HSET`` — and a reader that lands between two of them sees
        the write half applied. KV has no way to make that impossible: the state
        model would have to become one key per *name* holding the whole mapping,
        which buys the guarantee at the price of turning every single-field write
        on the order path into a read-modify-write.

        What it does instead is stop making the window wider than it has to be.
        Awaiting each put in turn spent one round trip per field with the state
        visibly half-written throughout; issuing them together spends one for the
        set. The ordering guarantee callers actually depend on — two writes to
        one name landing in the order they were issued — is the caller's lock
        above, not this loop, and different fields have no order between them.

        See the contract note on
        :meth:`~mftik.broker.transport.base.BrokerTransport.state_put_many` for
        what is and is not promised.
        """
        bucket = await self._bucket("state")
        await asyncio.gather(
            *(
                bucket.put(self._state_key(name, field), raw.encode())
                for field, raw in values.items()
            )
        )

    async def state_replace(self, name: str, values: Mapping[str, str]) -> None:
        """Write the new fields, then drop whatever the old set had extra.

        This order — rather than Redis' delete-then-write — is the one that
        keeps the promise that matters: a reader arriving between the two calls
        sees the new values plus possibly a stale one, never an empty state.
        Reversing it would give a strategy reading its ledger mid-replace an
        answer of "no balance", which it would act on.
        """
        async with self._state_lock(name):
            before = set(await self._all_fields(name))
            await self._put_fields(name, values)
            stale = before - set(values)
            if stale:
                await self._drop_fields(name, sorted(stale))

    async def state_get(self, name: str, field: str) -> str | None:
        bucket = await self._bucket("state")
        try:
            entry = await bucket.get(self._state_key(name, field))
        except nats.js.errors.KeyNotFoundError:
            return None
        return entry.value.decode() if entry.value is not None else None

    async def state_all(self, name: str) -> dict[str, str]:
        """Every field of ``name``, in one read of the bucket's own stream.

        Not ``keys()`` and then a get each: this is on the path a strategy takes
        to read its open orders, and an account with thirty of them would have
        paid thirty-one round trips for one answer. A consumer over the bucket's
        subject tree delivering the last message per subject is the same answer
        in one pass.
        """
        return await self._all_fields(name)

    async def _all_fields(self, name: str) -> dict[str, str]:
        bucket = await self._bucket("state")
        prefix = _kv_key(f"{name}.")
        rows = await self._kv_scan(bucket, f"{prefix}>")
        return {key[len(prefix) :]: raw for key, raw in rows.items()}

    async def _kv_scan(
        self, bucket: nats.js.kv.KeyValue, pattern: str
    ) -> dict[str, str]:
        """Every live key under ``pattern`` with its value, in one pass."""
        status = await bucket.status()
        stream = status.stream_info.config.name
        assert stream is not None
        head = f"$KV.{status.bucket}."
        subject = f"{head}{pattern}"
        # One message per key survives — the bucket keeps a history of one — so
        # the number of subjects is the number of keys, and that is the batch.
        subjects = await self._subject_counts(stream, subject)
        if not subjects:
            return {}
        rows: dict[str, str] = {}
        sub = await self.js.pull_subscribe(
            subject,
            stream=stream,
            config=js_api.ConsumerConfig(
                deliver_policy=js_api.DeliverPolicy.LAST_PER_SUBJECT,
                ack_policy=js_api.AckPolicy.NONE,
                filter_subject=subject,
                inactive_threshold=_READ_CONSUMER_IDLE_S,
            ),
        )
        try:
            remaining = len(subjects)
            while remaining > 0:
                try:
                    msgs = await sub.fetch(
                        batch=min(remaining, _READ_BATCH), timeout=_READ_TIMEOUT_S
                    )
                except (nats.errors.TimeoutError, TimeoutError):
                    break
                if not msgs:
                    break
                remaining -= len(msgs)
                for msg in msgs:
                    # A delete or a purge leaves a marker under the key, which is
                    # how a watcher learns the key went. To a reader it is simply
                    # absent — but it is still a message, so it is still one of
                    # the subjects counted above.
                    if (msg.headers or {}).get("KV-Operation") in ("DEL", "PURGE"):
                        continue
                    rows[msg.subject[len(head) :]] = msg.data.decode()
        finally:
            with contextlib.suppress(Exception):
                await sub.unsubscribe()
        return rows

    async def state_drop(self, name: str, fields: Sequence[str]) -> int:
        if not fields:
            return 0
        async with self._state_lock(name):
            return await self._drop_fields(name, fields)

    async def _drop_fields(self, name: str, fields: Sequence[str]) -> int:
        bucket = await self._bucket("state")
        dropped = 0
        for field in fields:
            key = self._state_key(name, field)
            try:
                await bucket.get(key)
            except nats.js.errors.KeyNotFoundError:
                continue
            # Purge rather than delete: a delete marker is a revision of its
            # own, and these keys are an order book being written per fill.
            await bucket.purge(key)
            dropped += 1
        return dropped

    async def state_clear(self, names: Sequence[str]) -> None:
        bucket = await self._bucket("state")
        for name in names:
            async with self._state_lock(name):
                for key in await self._kv_scan(bucket, f"{_kv_key(name)}.>"):
                    with contextlib.suppress(nats.js.errors.KeyNotFoundError):
                        await bucket.purge(key)

    # --- leases ------------------------------------------------------------
    #
    # The one family where this transport is not a translation of the Redis one
    # but an improvement on it. ``lease_hold`` and ``lease_release`` are
    # compare-and-set against the revision they read, so the race the Redis
    # implementation documents losing is not open here: a holder whose lease
    # lapsed and was taken by a rival is told no, rather than silently extending
    # the rival's claim.

    def _lease_subject(self, bucket_name: str, name: str) -> str:
        return f"$KV.{bucket_name}.{_kv_key(name)}"

    async def lease_put(
        self, name: str, *, ttl: float, owner: str = LEASE_ANONYMOUS
    ) -> None:
        bucket = await self._bucket("lease")
        status = await bucket.status()
        # Published rather than ``put``, because ``put`` takes no TTL: a lease
        # that outlived its holder is the one thing a lease may never do.
        await self.js.publish(
            self._lease_subject(status.bucket, name),
            owner.encode(),
            headers={js_api.Header.MSG_TTL: str(_ttl_seconds(ttl))},
        )

    async def lease_take(
        self, name: str, *, ttl: float, owner: str = LEASE_ANONYMOUS
    ) -> bool:
        bucket = await self._bucket("lease")
        try:
            await bucket.create(
                _kv_key(name), owner.encode(), msg_ttl=float(_ttl_seconds(ttl))
            )
        except (
            nats.js.errors.KeyWrongLastSequenceError,
            nats.js.errors.BadRequestError,
        ):
            return False
        return True

    async def lease_owner(self, name: str) -> str | None:
        entry = await self._lease_entry(name)
        return None if entry is None else entry[0]

    async def _lease_entry(self, name: str) -> tuple[str, int] | None:
        """``(owner, revision)``, or ``None`` when nobody holds ``name``."""
        bucket = await self._bucket("lease")
        try:
            entry = await bucket.get(_kv_key(name))
        except nats.js.errors.KeyNotFoundError:
            return None
        value = entry.value.decode() if entry.value is not None else ""
        return value, int(entry.revision or 0)

    async def lease_hold(self, name: str, *, owner: str, ttl: float) -> bool:
        held = await self._lease_entry(name)
        if held is None or held[0] != owner:
            return False
        bucket = await self._bucket("lease")
        status = await bucket.status()
        try:
            await self.js.publish(
                self._lease_subject(status.bucket, name),
                owner.encode(),
                headers={
                    js_api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE: str(held[1]),
                    js_api.Header.MSG_TTL: str(_ttl_seconds(ttl)),
                },
            )
        except nats.js.errors.APIError:
            # Somebody wrote between the read and here, so this caller is no
            # longer the holder it believed it was. Saying no is the whole
            # point: the rival keeps the resource and nothing was overwritten.
            return False
        return True

    async def lease_release(self, name: str, *, owner: str) -> bool:
        held = await self._lease_entry(name)
        if held is None or held[0] != owner:
            return False
        bucket = await self._bucket("lease")
        try:
            await bucket.delete(_kv_key(name), last=held[1])
        except (
            nats.js.errors.KeyWrongLastSequenceError,
            nats.js.errors.BadRequestError,
        ):
            return False
        return True

    async def lease_drop(self, name: str) -> None:
        bucket = await self._bucket("lease")
        with contextlib.suppress(
            nats.js.errors.KeyNotFoundError, nats.js.errors.BadRequestError
        ):
            await bucket.purge(_kv_key(name))

    # --- counters ----------------------------------------------------------

    async def counter_next(self, name: str) -> int:
        """Read, add one, write at the revision that was read. Retry on a loss.

        KV has no atomic increment. The server-side counter that would give one
        arrived in 2.12 and this node's floor is 2.11, so the increment is a
        compare-and-set loop instead — which is correct on any version, and
        cheap because the only caller allocates a slot once per session rather
        than per order.
        """
        bucket = await self._bucket("counter")
        key = _kv_key(name)
        for _attempt in range(16):
            try:
                entry = await bucket.get(key)
            except nats.js.errors.KeyNotFoundError:
                try:
                    await bucket.create(key, b"1")
                except (
                    nats.js.errors.KeyWrongLastSequenceError,
                    nats.js.errors.BadRequestError,
                ):
                    continue
                return 1
            try:
                current = int((entry.value or b"0").decode())
            except ValueError:
                current = 0
            nxt = current + 1
            try:
                await bucket.update(
                    key, str(nxt).encode(), last=int(entry.revision or 0)
                )
            except (
                nats.js.errors.KeyWrongLastSequenceError,
                nats.js.errors.BadRequestError,
            ):
                continue
            return nxt
        raise RuntimeError(
            f"counter {name!r} lost sixteen races in a row; something is "
            f"incrementing it far faster than it was designed for"
        )

    # --- recorded tape -----------------------------------------------------
    #
    # A stream per feed, which is the one place this transport creates streams
    # on the fly. Two reasons, and the second is the load-bearing one.
    #
    # The retention bounds are per feed in the interface — ``maxlen`` records
    # and ``ttl_seconds`` of age — and a stream's limits are the stream's, so
    # one stream for every feed could only ever hold the loosest of them.
    #
    # And a warm-up asks for "the newest N records of this feed", which a
    # consumer answers by starting at a sequence. Sequences are the stream's, so
    # on a shared stream the arithmetic would have to skip over every other
    # feed's prints — hundreds of thousands of them on a busy node — to find
    # where this feed's last N began. On its own stream the answer is
    # subtraction.

    def _tape_stream(self, feed: str) -> str:
        return self._named("tape", feed)

    async def _ensure_tape_stream(
        self, feed: str, *, maxlen: int, ttl_seconds: int
    ) -> str:
        name = self._tape_stream(feed)
        await self._ensure_stream(
            js_api.StreamConfig(
                name=name,
                subjects=[self._tape_subject(feed)],
                retention=js_api.RetentionPolicy.LIMITS,
                discard=js_api.DiscardPolicy.OLD,
                max_msgs=maxlen,
                max_age=ttl_seconds,
                allow_direct=True,
            )
        )
        return name

    async def tape_append(
        self,
        feed: str,
        fields: Mapping[str, str],
        *,
        maxlen: int,
        ttl_seconds: int,
        recorded_ms: int | None = None,
    ) -> None:
        await self._ensure_tape_stream(feed, maxlen=maxlen, ttl_seconds=ttl_seconds)
        headers = (
            None if recorded_ms is None else {RECORDED_MS_HEADER: str(recorded_ms)}
        )
        await self.js.publish(
            self._tape_subject(feed),
            json.dumps(dict(fields)).encode(),
            headers=headers,
        )

    async def _tape_newest(self, feed: str, *, count: int) -> list[tuple[int, str]]:
        """The newest ``count`` records of ``feed``, by sequence arithmetic.

        Only sound because a feed owns its stream outright: sequences are the
        stream's, so ``last_seq - count + 1`` is this feed's ``count``-from-the-end
        and nothing else's. The same arithmetic on a shared stream reads a window
        of whatever was written last, which is generally somebody else's — see
        :meth:`fetch_log_buffer` for how a shared subject is read instead.
        """
        stream = self._tape_stream(feed)
        subject = self._tape_subject(feed)
        try:
            info = await self.js.stream_info(stream, subjects_filter=subject)
        except nats.js.errors.NotFoundError:
            return []
        held = (info.state.subjects or {}).get(subject, 0)
        if not held:
            return []
        want = min(count, held)
        start = max(info.state.first_seq, info.state.last_seq - want + 1)
        return await self._read(
            stream,
            subject,
            expected=want,
            config=js_api.ConsumerConfig(
                deliver_policy=js_api.DeliverPolicy.BY_START_SEQUENCE,
                opt_start_seq=start,
                ack_policy=js_api.AckPolicy.NONE,
                filter_subject=subject,
                inactive_threshold=_READ_CONSUMER_IDLE_S,
            ),
        )

    async def tape_tail(
        self, feed: str, *, count: int
    ) -> list[tuple[int, dict[str, str]]]:
        if count <= 0:
            return []
        rows = await self._tape_newest(feed, count=count)
        out: list[tuple[int, dict[str, str]]] = []
        for stamped_ms, raw in rows:
            try:
                fields = json.loads(raw)
            except ValueError:
                logger.warning("tape record on %s will not parse", feed)
                continue
            out.append((stamped_ms, {str(k): str(v) for k, v in fields.items()}))
        return out

    async def tape_trim_before(self, feed: str, *, min_id_ms: int) -> int:
        """Purge everything stamped before ``min_id_ms``. How many went.

        Swept rather than declarative, even though the stream has a ``max_age``
        that would eventually do it. The age is the backstop the caller asked
        for — twice the window — while this is the window itself, and letting
        the backstop stand in for it would hand a warm-up twice the history the
        operator configured. That is a silent policy change, so it is done
        properly: find the first record at or after the horizon, and purge up to
        it.
        """
        name = self._tape_stream(feed)
        subject = self._tape_subject(feed)
        try:
            info = await self.js.stream_info(name)
        except nats.js.errors.NotFoundError:
            return 0
        before = info.state.messages
        if not before:
            return 0

        horizon = dt.datetime.fromtimestamp(min_id_ms / 1000, tz=dt.UTC)

        # "Is anything new enough to keep" is asked of the newest record itself
        # rather than inferred from a read that came back with nothing. The two
        # used to be the same answer, and that made a slow server indistinguish-
        # able from a feed that stopped printing an hour ago — one sweep that
        # timed out purged the whole warm-up window and reported it as a trim.
        if await self._is_older_than(name, subject, horizon):
            # ``seq`` purges up to but not including, so the sequence after the
            # last one is how "all of it" is spelled.
            await self.js.purge_stream(name, seq=info.state.last_seq + 1)
        else:
            first_kept = await self._first_seq_at_or_after(name, subject, horizon)
            if first_kept is None:
                # Something is inside the window and we could not find where it
                # starts. Doing nothing costs one sweep; the next one is a minute
                # away and the stream's ``max_age`` is the backstop underneath.
                logger.warning(
                    "broker tape trim found no horizon on %s — leaving it", feed
                )
                return 0
            if first_kept <= info.state.first_seq:
                return 0
            await self.js.purge_stream(name, seq=first_kept)
        after = (await self.js.stream_info(name)).state.messages
        return max(0, before - after)

    async def _is_older_than(
        self, stream: str, subject: str, when: dt.datetime
    ) -> bool:
        """Whether every message on ``subject`` predates ``when``.

        One direct read of the newest record, which either answers or raises.
        That is the whole point of doing it this way: a consumer that finds
        nothing is ambiguous, and this is the question whose wrong answer purges
        a feed's entire history.
        """
        try:
            newest = await self.js.get_last_msg(stream, subject)
        except nats.js.errors.NotFoundError:
            return False
        return newest.time is not None and newest.time < when

    async def _first_seq_at_or_after(
        self, stream: str, subject: str, when: dt.datetime
    ) -> int | None:
        """The sequence of the first message on ``subject`` stamped at ``when``.

        ``None`` means *could not tell*, and nothing else — not "there is none".
        Callers establish that separately with :meth:`_is_older_than` before
        asking, because a consumer delivering nothing and a fetch that timed out
        look identical from here and only one of them means the feed is empty of
        anything worth keeping.
        """
        sub = await self.js.pull_subscribe(
            subject,
            stream=stream,
            config=js_api.ConsumerConfig(
                deliver_policy=js_api.DeliverPolicy.BY_START_TIME,
                opt_start_time=when,
                ack_policy=js_api.AckPolicy.NONE,
                filter_subject=subject,
                inactive_threshold=_READ_CONSUMER_IDLE_S,
            ),
        )
        try:
            msgs = await sub.fetch(batch=1, timeout=_READ_TIMEOUT_S)
        except (nats.errors.TimeoutError, TimeoutError):
            return None
        finally:
            with contextlib.suppress(Exception):
                await sub.unsubscribe()
        if not msgs:
            return None
        return msgs[0].metadata.sequence.stream

    async def tape_coverage(self, feed: str) -> dict[str, str]:
        bucket = await self._bucket("tapecov")
        try:
            entry = await bucket.get(_kv_key(feed))
        except nats.js.errors.KeyNotFoundError:
            return {}
        try:
            stored = json.loads((entry.value or b"{}").decode())
        except ValueError:
            return {}
        return {str(k): str(v) for k, v in stored.items()}

    async def tape_coverage_put(
        self, feed: str, values: Mapping[str, str], *, ttl_seconds: int
    ) -> None:
        """Merge ``values`` into this feed's coverage.

        One key holding the whole record rather than a key per field, because
        every reader wants all of it and the writer replaces most of it at once.
        Read-modify-write, which is safe for the same reason the broker's
        continuity arithmetic above it is: a feed has exactly one recorder.
        """
        bucket = await self._bucket("tapecov")
        status = await bucket.status()
        merged = {
            **await self.tape_coverage(feed),
            **{k: str(v) for k, v in values.items()},
        }
        await self.js.publish(
            f"$KV.{status.bucket}.{_kv_key(feed)}",
            json.dumps(merged).encode(),
            headers={js_api.Header.MSG_TTL: str(_ttl_seconds(ttl_seconds))},
        )


def _stamp_ms(msg: Msg) -> int:
    """When a stored message was recorded, in milliseconds.

    The server's own stamp unless the writer named one, which is the clock the
    tape's continuity marks are measured against. Not the venue's — that rides
    on the record as a field and answers a different question.

    :meth:`NatsTransport.tape_trim_before` asks the server to find a horizon by
    time, so it reads the server's stamp even for a record carrying a header.
    The two agree to within a round trip for anything actually being recorded;
    a record placed at an arbitrary stamp is a test's, and trimming is not what
    it is testing.
    """
    named = (msg.headers or {}).get(RECORDED_MS_HEADER)
    if named is not None:
        try:
            return int(named)
        except ValueError:
            pass
    try:
        return int(msg.metadata.timestamp.timestamp() * 1000)
    except Exception:
        return 0


async def _iter_until_stopped(
    inbound: asyncio.Queue[tuple[str, str | None]] | asyncio.Queue[tuple[str, str]],
    *,
    stop: asyncio.Event | None,
) -> AsyncIterator:
    """Yield from ``inbound`` until ``stop`` is set.

    Racing the queue against the stop event, rather than polling the queue with
    a timeout, is what makes a NATS serve loop stop the moment it is told to.
    The Redis transport cannot do this — a blocking pop is not cancellable
    without losing whatever it was about to return — and waiting out one poll on
    every teardown is the cost the whole test suite used to pay for it.
    """
    if stop is None:
        while True:
            yield await inbound.get()

    stopping = asyncio.ensure_future(stop.wait())
    try:
        while not stop.is_set():
            nxt = asyncio.ensure_future(inbound.get())
            done, _pending = await asyncio.wait(
                {nxt, stopping}, return_when=asyncio.FIRST_COMPLETED
            )
            if nxt in done:
                yield nxt.result()
                continue
            nxt.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await nxt
            break
        # Whatever arrived before the event was set is still work this process
        # accepted: ``_pump_posted`` acknowledges a posted message as it hands
        # it over, so one dropped here is a backfill or an account sweep that
        # the queue will not offer to anybody again.
        #
        # Drained after the loop rather than inside the stopping branch, because
        # both ways out need it. A message that *wins* the race against the stop
        # event is yielded and the loop goes round to a condition that is now
        # false — which used to leave everything queued behind that one message
        # unread, and that is the likelier ordering of the two: a plane is
        # usually told to stop while its subject is busy.
        while not inbound.empty():
            yield inbound.get_nowait()
    finally:
        stopping.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stopping
