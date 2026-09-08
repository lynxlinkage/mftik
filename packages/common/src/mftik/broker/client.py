"""The broker — what a plane may say, over whichever transport it was given."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import Any

from pydantic import BaseModel

from mftik.broker.config import BrokerConfig
from mftik.broker.request import IncomingRequest
from mftik.broker.stream import BidirectionalStream
from mftik.broker.transport import build as build_transport
from mftik.broker.transport.base import LEASE_ANONYMOUS, BrokerTransport
from mftik.protocol import (
    Envelope,
    Heartbeat,
    HeartbeatEnvelope,
    Topics,
    UntypedEnvelope,
)

logger = logging.getLogger(__name__)

Handler = Callable[[IncomingRequest], Awaitable[None]]


def _to_json(value: BaseModel | dict[str, Any]) -> str:
    if isinstance(value, BaseModel):
        return value.model_dump_json()
    return json.dumps(value, default=str)


#: How many measured gaps one feed's coverage will carry before the tape stops
#: being described as one series at all. A feed that was interrupted this often
#: inside its retention window is not a recording with holes in it, and the
#: field is one value in a coverage record, not a history — it does not get to
#: grow forever.
TAPE_MAX_GAPS = 32


def encode_tape_gaps(gaps: Sequence[tuple[int, int]]) -> str:
    """Render measured gaps as ``start-end`` pairs, oldest first.

    A flat string rather than JSON: these are pairs of integers written on
    every feed restart and read on every warm-up, and the coverage record is
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


class Broker:
    """Async IPC client — the whole of what a plane may say.

    Six processes, none of which import each other, and this is what they
    share. Every family below is documented in ``docs/Broker.md`` along with
    what each transport does to answer it; the short version is that fan-out is
    best effort, request-reply waits for a consumer, and a lease expires.

    The store underneath is a :class:`~mftik.broker.transport.base.BrokerTransport`
    chosen by ``BROKER_TRANSPORT``. Nothing above this class may know which one
    it got — that is the rule
    ``packages/common/tests/test_broker_is_the_only_transport.py`` enforces, and
    the reason this class exists rather than callers holding a transport.

    What lives here rather than in a transport is everything that would
    otherwise have been written twice: envelope encoding, the continuity
    arithmetic in :meth:`tape_mark_recording`, and the two small objects that
    wrap a subject pair and an incoming request.
    """

    def __init__(
        self,
        config: BrokerConfig | None = None,
        *,
        transport: BrokerTransport | None = None,
    ) -> None:
        self.config = config or BrokerConfig.from_env()
        self._transport = transport or build_transport(self.config)

    # --- lifecycle ---------------------------------------------------------

    @property
    def transport(self) -> BrokerTransport:
        """The store this broker is talking to.

        Here for the tests that need to break a transport on purpose to prove a
        serve loop survives it. A domain reaching this is going around the
        broker, and the guard test says so by name.
        """
        return self._transport

    async def connect(self) -> None:
        await self._transport.connect()
        logger.info("Connected to %s", self._transport.describe())

    async def close(self) -> None:
        await self._transport.close()

    async def __aenter__(self) -> Broker:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # --- shared state ------------------------------------------------------
    #
    # Fan-out tells a reader that something changed; these hold what it changed
    # *to*. A late subscriber, a restarted process and a strategy that missed a
    # message all read the same current answer here, which is what makes "the
    # writer's state and the reader's state agree" true by construction rather
    # than by both sides keeping their own copy in sync.

    async def state_put(
        self, name: str, field: str, value: BaseModel | dict[str, Any]
    ) -> None:
        """Write one field of a shared state."""
        await self._transport.state_put_many(name, {field: _to_json(value)})

    async def state_put_many(
        self, name: str, values: Mapping[str, BaseModel | dict[str, Any]]
    ) -> None:
        """Write several fields, leaving the rest alone."""
        if not values:
            return
        await self._transport.state_put_many(
            name, {k: _to_json(v) for k, v in values.items()}
        )

    async def state_replace(
        self, name: str, values: Mapping[str, BaseModel | dict[str, Any]]
    ) -> None:
        """Make the state exactly ``values`` — the recon path.

        A reader never observes the empty gap between the old contents and the
        new. Whether it can briefly see both is the transport's business; see
        :meth:`BrokerTransport.state_replace`.
        """
        await self._transport.state_replace(
            name, {k: _to_json(v) for k, v in values.items()}
        )

    async def state_get(self, name: str, field: str) -> dict[str, Any] | None:
        raw = await self._transport.state_get(name, field)
        return None if raw is None else json.loads(raw)

    async def state_all(self, name: str) -> dict[str, dict[str, Any]]:
        rows = await self._transport.state_all(name)
        return {field: json.loads(raw) for field, raw in rows.items()}

    async def state_drop(self, name: str, *fields: str) -> int:
        return await self._transport.state_drop(name, fields)

    async def state_clear(self, *names: str) -> None:
        """Delete whole states — call when their owner goes away.

        State that outlives its writer is worse than no state: a reader cannot
        tell a stale answer from a current one.
        """
        await self._transport.state_clear(names)

    # --- leases (claims that expire) ---------------------------------------
    #
    # A lease is a fact about *now* that its holder may never get to retract:
    # a process running a session, holding an account, walking an account's
    # history. The states above are deleted by their owner, which covers every
    # ending the owner is around to observe and not the one that matters here —
    # SIGKILL, OOM, the machine going away — after which a fact with no expiry
    # is a session the UI shows as running that nobody can stop.
    #
    # So a lease always expires, and its holder renews it while it lives.
    #
    # Three of them decide *who* holds a resource, which is why they are the
    # transport's methods rather than a read and a write composed here: a
    # decision that takes two round trips has a race in the middle, and where
    # that race can be closed is underneath. Redis cannot close it and says so;
    # NATS does, and no caller changed.

    async def lease_put(
        self, name: str, *, ttl: float, owner: str = LEASE_ANONYMOUS
    ) -> None:
        """Write a lease, expiring ``ttl`` seconds from now.

        Unconditional: this states a fact rather than asking a question, which
        is right for a holder renewing its own lease and wrong for anything
        deciding who the holder is. A heartbeat that checked first would stop
        renewing the moment its own key lapsed, when re-taking it is exactly
        what it wants; :meth:`lease_take` is for the other question.

        ``ttl`` is honoured to the millisecond on Redis and rounded up to whole
        seconds on NATS, whose per-message TTL has a one second floor. The
        leases in production are thirty seconds.
        """
        await self._transport.lease_put(name, ttl=ttl, owner=owner)

    async def lease_take(
        self, name: str, *, ttl: float, owner: str = LEASE_ANONYMOUS
    ) -> bool:
        """Take a lease only if nobody holds one. Whether it was taken.

        The atomic half of a claim, and the only part of one that cannot be
        assembled from a read and a write. Several processes of a plane come up
        together and each asks for the same session; without the test being
        part of the write they are all told yes and all run it.

        A refusal says nothing about who refused it — ask :meth:`lease_owner`.
        """
        return await self._transport.lease_take(name, ttl=ttl, owner=owner)

    async def lease_owner(self, name: str) -> str | None:
        """Who holds ``name``, or ``None`` when nobody does.

        The value :meth:`lease_put` wrote, so a lease taken without naming a
        holder answers :data:`LEASE_ANONYMOUS` rather than anything useful.
        """
        return await self._transport.lease_owner(name)

    async def lease_held(self, name: str) -> bool:
        """Whether anybody holds ``name``.

        For the leases that answer "is a process still here" rather than
        "which process" — the reader has no holder to compare against, and
        would only be checking that the string it got back was not empty.
        """
        return await self.lease_owner(name) is not None

    async def lease_hold(self, name: str, *, owner: str, ttl: float) -> bool:
        """Extend a lease still held by ``owner``. ``False`` when it is not.

        A missing lease is never re-created here. Whoever let one expire goes
        back through :meth:`lease_take`, where a rival can say no.

        The two transports disagree about how tightly this holds, and it is
        worth knowing which one is underneath. NATS extends at the revision it
        read, so a lease that lapsed and was re-taken by a rival is refused.
        Redis has no compare-and-set to lean on and loses that race in the safe
        direction: it extends the rival's lease by one period rather than taking
        it back, so the rival keeps the resource and this caller finds out on
        its next pass.
        """
        return await self._transport.lease_hold(name, owner=owner, ttl=ttl)

    async def lease_release(self, name: str, *, owner: str) -> bool:
        """Give up a lease, if it is still ``owner``'s to give up.

        Conditional because a process shutting down may already have lost its
        lease to the one that replaced it, and deleting a stranger's claim on
        the way out hands the resource to a third process while the second still
        believes it holds it.

        Releasing rather than waiting out the TTL is what keeps a redeploy from
        looking like an outage nobody caused.
        """
        return await self._transport.lease_release(name, owner=owner)

    async def lease_drop(self, name: str) -> None:
        """Delete a lease, whoever holds it. Safe when there is none.

        The counterpart to :meth:`lease_put`'s unnamed holder: a lease nobody
        signed cannot be released conditionally, because "is it still mine" has
        no answer to check. Callers that named themselves want
        :meth:`lease_release` instead.
        """
        await self._transport.lease_drop(name)

    # --- shared counters ---------------------------------------------------

    async def counter_next(self, name: str) -> int:
        """Increment a shared counter and return the value that came back.

        Shared rather than process-local because the callers are competing
        consumers: several processes of a plane serve one subject, so a
        counter each would hand two of them the same number.

        Monotonic and unbounded. Folding it into a range is the caller's job,
        and how wide that range is decides how long it takes a value to
        repeat — see STS's cid slot, where a repeat is harmless anyway.
        """
        return await self._transport.counter_next(name)

    # --- recorded tape -----------------------------------------------------
    #
    # A feed's own history, kept so a strategy that starts later can warm up on
    # what it missed.
    #
    # Two bounds, and they mean different things. ``maxlen`` on append is the
    # memory fuse. The trim is the intent. Whichever binds first is what the
    # reader gets, and :meth:`tape_coverage` is how it finds out which.

    async def tape_append(
        self,
        feed: str,
        fields: Mapping[str, str],
        *,
        maxlen: int,
        ttl_seconds: int,
    ) -> None:
        """Append one record, capping the feed at ``maxlen`` entries.

        The record's stamp is the broker's clock, not the venue's timestamp.
        Event time is a field on the record instead, because a venue tape is not
        strictly monotonic and one late print out of a million should not be
        able to end a recording.

        ``ttl_seconds`` is renewed on every append, so a feed that stops being
        recorded expires on its own. Without it a tape would outlive the last
        strategy that ever wanted it.
        """
        await self._transport.tape_append(
            feed, fields, maxlen=maxlen, ttl_seconds=ttl_seconds
        )

    async def tape_tail(
        self, feed: str, *, count: int
    ) -> list[tuple[int, dict[str, str]]]:
        """Read the newest ``count`` records as ``(recorded_ms, fields)``.

        Oldest → newest, because a warm-up replays forward. The *newest*
        ``count`` rather than the oldest: warming up means catching up to now,
        and a tape held by two independent bounds contains an unknown number of
        records, so "the first N" is not a window anyone asked for.

        ``recorded_ms`` is the broker's clock at append time, which is the stamp
        :meth:`tape_coverage`'s continuity mark is measured against. Not the
        venue's timestamp — that rides on the record as a field and answers a
        different question; the two are not interchangeable.
        """
        if count <= 0:
            return []
        return await self._transport.tape_tail(feed, count=count)

    async def tape_trim_before(self, feed: str, *, min_id_ms: int) -> int:
        """Drop records older than ``min_id_ms``. Returns how many went."""
        return await self._transport.tape_trim_before(feed, min_id_ms=min_id_ms)

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
        is sitting intact in the recording — the warm-up window, thrown away to
        describe a hole shorter than one bar. What cannot be measured is still
        treated as fatal to continuity; what can is handed over as a fact.

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

        await self._transport.tape_coverage_put(
            feed,
            {
                "continuous_since_ms": str(continuous_since),
                "recording": "1",
                "stopped_ms": "",
                "gaps": encode_tape_gaps(gaps),
            },
            ttl_seconds=ttl_seconds,
        )

    async def tape_mark_stopped(
        self, feed: str, *, at_ms: int, ttl_seconds: int
    ) -> None:
        """Record that this feed stopped recording at ``at_ms``.

        The records are left alone. A reader that wants the last two hours
        before a feed went quiet can still have them — it just has to know they
        end, and that is exactly what this says.

        It is also the near edge of any gap that follows. Only a recorder that
        got to run its shutdown leaves this behind, which is what makes a
        planned interruption measurable and an unplanned one not — see
        :meth:`tape_mark_recording`.

        ``ttl_seconds`` for the same reason as the other coverage write: a
        transport that stores this as a record with an expiry has to be told
        what the expiry is, and the recorder is the only thing that knows.
        """
        await self._transport.tape_coverage_put(
            feed,
            {"recording": "0", "stopped_ms": str(at_ms)},
            ttl_seconds=ttl_seconds,
        )

    async def tape_coverage(self, feed: str) -> dict[str, str]:
        """What this feed's tape covers, or ``{}`` if it was never recorded."""
        return await self._transport.tape_coverage(feed)

    # --- fan-out -----------------------------------------------------------

    async def publish(self, topic: str, envelope: Envelope[Any]) -> None:
        """Publish an envelope to a fan-out topic.

        Nothing comes back. Fan-out here is best effort by design: a message
        published while nobody is subscribed is gone, and where that is not
        acceptable the topic is written through :meth:`publish_log` or the
        caller is using request-reply instead.
        """
        await self._transport.publish(topic, envelope.to_json())

    async def publish_log(
        self,
        topic: str,
        envelope: Envelope[Any],
        *,
        maxlen: int | None = None,
        ttl_seconds: int = 86_400,
    ) -> None:
        """Publish a log line and keep the last few for late subscribers.

        Fan-out alone drops messages when nobody is listening (e.g. the UI
        opens ``/ws/sts/...`` after a deploy). The buffer is replayed on
        connect by :meth:`fetch_log_buffer`. ``maxlen`` defaults to
        :attr:`BrokerConfig.log_buffer_maxlen` (100).
        """
        keep = self.config.log_buffer_maxlen if maxlen is None else max(1, maxlen)
        await self._transport.publish_log(
            topic, envelope.to_json(), maxlen=keep, ttl_seconds=ttl_seconds
        )

    async def fetch_log_buffer(self, topic: str) -> list[str]:
        """Return buffered log JSON lines for ``topic`` (oldest → newest)."""
        return await self._transport.fetch_log_buffer(topic)

    async def subscribe(
        self,
        topics: str | Sequence[str],
        *,
        stop: asyncio.Event | None = None,
    ) -> AsyncIterator[UntypedEnvelope]:
        """Yield envelopes from one or more fan-out topics until ``stop``.

        Messages published while not subscribed are lost unless they were also
        written via :meth:`publish_log`.
        """
        topic_list = (topics,) if isinstance(topics, str) else tuple(topics)
        if not topic_list:
            raise ValueError("subscribe requires at least one topic")
        async for _topic, raw in self._transport.subscribe(topic_list, stop=stop):
            yield UntypedEnvelope.from_json(raw)

    async def psubscribe(
        self,
        patterns: str | Sequence[str],
        *,
        stop: asyncio.Event | None = None,
    ) -> AsyncIterator[tuple[str, UntypedEnvelope]]:
        """Yield ``(topic, envelope)`` from pattern subscriptions until ``stop``.

        Messages published while not subscribed are lost unless they were also
        written via :meth:`publish_log`, exactly as in :meth:`subscribe`.

        Patterns belong on :class:`~mftik.protocol.Topics` and must use one
        wildcard per segment — ``log.*.*``, not ``log.*``. Redis globs the
        whole channel name and would match both; a transport that matches per
        segment matches only the first, and a pattern that matches nothing
        fails silently. See the note above ``Topics.log_pattern``.
        """
        pattern_list = (patterns,) if isinstance(patterns, str) else tuple(patterns)
        if not pattern_list:
            raise ValueError("psubscribe requires at least one pattern")
        async for topic, raw in self._transport.psubscribe(pattern_list, stop=stop):
            yield topic, UntypedEnvelope.from_json(raw)

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

        The envelope's ``id`` is the correlation id. Where the reply is
        addressed is the transport's business — some put it in the envelope on
        the way out, some carry it beside the message — so all this does is ask
        and stamp what it is told.

        A :class:`~mftik.broker.errors.RequestTimeoutError` is the caller's
        answer of "down" at least as often as it is a fault.
        """
        wait = self.config.request_timeout if timeout is None else timeout
        outbound = self._addressed(envelope)
        raw = await self._transport.request(
            subject,
            outbound.to_json(),
            request_id=outbound.id,
            inbox=outbound.reply_to,
            timeout=wait,
        )
        return UntypedEnvelope.from_json(raw)

    async def probe(
        self,
        subject: str,
        envelope: Envelope[Any],
        *,
        timeout: float | None = None,
    ) -> UntypedEnvelope:
        """Ask whether somebody is serving ``subject``, leaving nothing behind.

        :meth:`request` on a subject nobody serves may leave the request for the
        next consumer, which every other caller wants: an attach or a backfill
        parked until its owner comes up is recovery. A liveness probe is the one
        request where that is not recovery but litter — a dashboard polling a
        down instance would write a record per probe that nobody will ever
        drain, and the instance, when it finally booted, would open by answering
        a heap of questions nobody is still waiting on.

        So whatever a transport does to make an unserved request wait, it does
        not do it here. The reply path is :meth:`request`'s exactly, and a
        timeout still raises :class:`RequestTimeoutError`, which is the caller's
        answer of "down".
        """
        wait = self.config.request_timeout if timeout is None else timeout
        outbound = self._addressed(envelope)
        raw = await self._transport.probe(
            subject,
            outbound.to_json(),
            request_id=outbound.id,
            inbox=outbound.reply_to,
            timeout=wait,
        )
        return UntypedEnvelope.from_json(raw)

    def _addressed(self, envelope: Envelope[Any]) -> Envelope[Any]:
        """``envelope`` carrying the reply address, if this transport uses one."""
        inbox = self._transport.reply_inbox(envelope.id)
        if inbox is None or envelope.reply_to == inbox:
            return envelope
        return envelope.model_copy(update={"reply_to": inbox})

    async def post(self, subject: str, envelope: Envelope[Any]) -> None:
        """Enqueue on a request-reply subject without waiting for a reply.

        The same subject :meth:`request` uses and the same competing consumers
        take from it; what is missing is the ``reply_to``, so the handler
        answers nobody and this returns as soon as the work has been accepted.

        For work whose *result* the sender has no use for and whose duration it
        must not inherit — a backfill run is minutes of venue round trips, and
        the shutdown path that asks for one is measured in seconds. A request
        left because nothing is serving the subject yet is not lost: the next
        consumer to come up takes it, which is the recovery a fan-out message
        could not offer, and the one place both transports pay for a durable
        queue to keep that promise.
        """
        await self._transport.post(subject, envelope.to_json())

    async def serve(
        self,
        subject: str,
        *,
        stop: asyncio.Event | None = None,
    ) -> AsyncIterator[IncomingRequest]:
        """Yield incoming requests on a request-reply subject.

        Call ``await req.reply(envelope)`` to respond. Competing consumers on
        the same subject share the work.

        Only ``stop`` ends this loop. Neither a transport hiccup nor a message
        that will not parse does, because this generator *is* a domain's control
        plane: when it returns, the process stays up, the sessions keep trading
        and every request piles up unread — the most expensive way a service can
        fail, and the quietest.
        """
        async for raw, inbox in self._transport.serve(subject, stop=stop):
            try:
                envelope = UntypedEnvelope.from_json(raw)
            except Exception:
                # The transport has already taken it, so there is nothing to
                # skip past and no way to hand it back: the choice is to drop
                # this one message or to take the whole subject down with it.
                logger.exception(
                    "broker serve dropped an unreadable request subject=%s",
                    subject,
                )
                continue
            if inbox is not None and envelope.reply_to != inbox:
                # A transport that carries the reply address beside the message
                # rather than inside it. Stamping it here is what lets a handler
                # read ``req.envelope.reply_to`` without knowing which.
                envelope = envelope.model_copy(update={"reply_to": inbox})
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
        await self._transport.send_reply(reply_to, envelope.to_json())

    # --- convenience -------------------------------------------------------

    async def heartbeat_loop(
        self,
        source: str,
        *,
        interval: float = 5.0,
        stop: asyncio.Event | None = None,
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        """Publish periodic heartbeats on the heartbeat fan-out topic."""
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
