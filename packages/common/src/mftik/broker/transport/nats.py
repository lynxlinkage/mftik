"""The NATS transport — subjects, streams, consumers and KV buckets.

Every family the broker speaks maps onto something NATS already has, and the
mapping is worth stating in one place because three of the choices are not the
obvious one.

**Live fan-out is core NATS. The stream is the tail, not the bus.** A
``js.publish`` ack comes from the stream's leader, and a stream lives in
one cluster. Pinning that cluster to JP makes every TW publisher wait;
leaving it on TW makes every JP publisher wait. Core publish and
subscribe wait for the server this process connected to, and the gateway
forwards interest to the other cluster. The fan-out stream still captures
the same subjects so a later reader can address a subject by offset —
that is why ``connect`` still ensures it, not because ``nc.publish``
would fail without one. :meth:`NatsTransport.subscribe` starts at *now*,
which is the broker's promise: a message published while nobody was
subscribed is gone.

The tape still ``js.publish``. Session logs and status are core
``publish``; a late WebSocket reads ``session_logs`` or the session
list, not a stream ring. ``connect`` no longer ensures ``{prefix}_log``.

**Request-reply is core NATS.** A caller waiting on an answer gains nothing
from durability: it has a timeout, and a request executed after that timeout
passed is a side effect nobody is expecting any more. Core request-reply also
answers *better* — no responders is an immediate error rather than five seconds
of silence, so the control plane learns that a plane is down in milliseconds.

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
import dataclasses
import datetime as dt
import json
import logging
import re
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

import nats
import nats.errors
import nats.js.api as js_api
import nats.js.errors
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

from mftik.broker.config import BrokerConfig
from mftik.broker.errors import (
    BrokerNotConnectedError,
    RequestTimeoutError,
    StateReadIncompleteError,
    StreamShapeError,
)
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

#: How long the fan-out stream keeps a subject's messages. A fuse, not a
#: log: live subscribers start at now, and a quiet topic's leftover prints
#: should not sit forever.
FANOUT_MAX_AGE_SECONDS = 86_400

#: How many messages one fan-out subject keeps. Live pub/sub is not a log:
#: subscribers start at now, and this cap is only a fuse so a busy feed
#: cannot grow the stream. Session-log late replay is ``session_logs``.
FANOUT_MAX_MSGS_PER_SUBJECT = 256

#: Ceiling on a log ring. STS and the API ask for 200 for the session status
#: ring; ``BrokerConfig.log_buffer_maxlen`` defaults to 100. The log stream's
#: ``max_msgs_per_subject`` is this number so both fit without a purge.
LOG_MAX_MSGS_PER_SUBJECT = 256

#: Global fuse on the fan-out stream, in messages. Reached only if subjects
#: themselves multiply without end, so it is what a session-churn bug hits
#: instead of the disk.
FANOUT_MAX_MSGS = 1_000_000

#: How long a KV replica/placement update may take. The RPC default is five
#: seconds, which is enough for an empty bucket and not for a full ledger
#: copying onto two new peers.
_KV_RESHAPE_TIMEOUT_S = 60.0

#: How long to wait before re-asking a subject that reported no responders.
_NO_RESPONDERS_GRACE_S = 0.05

#: How much of a caller's own timeout may go on re-asking, and the bounds on
#: that. A share rather than a count of attempts, because what "no responders"
#: is worth waiting through depends entirely on who is asking.
#:
#: A fixed ~100ms was wrong in one direction: order entry gets
#: ``ORDER_ACK_TIMEOUT_S`` — two seconds — and an account loop being handed from
#: one TD process to another takes longer than a tenth of a second, so a fill
#: that Redis would have parked through the handover failed here instead. It
#: would be wrong in the other direction too if it were simply "the whole
#: timeout": the reason request-reply is core NATS and not a JetStream queue is
#: that a plane which is genuinely down is known to be down at once.
#:
#: So: half of what the caller brought, never below the boot race this exists to
#: cover, and never above a second — a twenty second control-plane request still
#: comes back saying nobody is there while its caller has nineteen seconds left
#: to decide what to do about it.
_NO_RESPONDERS_SHARE = 0.5
_NO_RESPONDERS_FLOOR_S = 0.1
_NO_RESPONDERS_CEILING_S = 1.0

#: How long a read's fetch waits before giving up. Sized against a server one
#: round trip away and never reached in the normal case, because every read
#: below knows how many messages it is asking for.
_READ_TIMEOUT_S = 2.0

#: How many messages one fetch asks for at most.
_READ_BATCH = 256

#: How long a read's consumer survives if this process dies mid-read. A
#: backstop, not the policy: :meth:`NatsTransport._close_reader` deletes the
#: consumer when the read is done, and this only covers the process that never
#: gets there.
_READ_CONSUMER_IDLE_S = 30.0

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
        # Last field set this process wrote for a state name. A name has one
        # writer, so ``state_replace`` can drop the extras without reading first.
        self._state_fields: dict[str, set[str]] = {}
        # ``bucket.status()`` is a raft read. Cached per kind after first use.
        self._kv_status: dict[str, Any] = {}

    # --- names -------------------------------------------------------------

    @property
    def _prefix(self) -> str:
        return self.config.key_prefix

    def _fanout_subject(self, topic: str) -> str:
        return f"{self._prefix}.ps.{_check_subject(topic)}"

    def _rpc_subject(self, subject: str) -> str:
        """Where a live request is asked. Core NATS, and no stream over it.

        A JetStream stream is a subscriber like any other, so a stream whose
        filter covered this subject would receive every core request — and
        answer it, on the requester's own reply subject, with a publish
        acknowledgement. The caller then parses ``{"stream": ..., "seq": 1}``
        as the reply it was waiting for. Fan-out and logs stay on their own
        subject spaces for that reason.
        """
        return f"{self._prefix}.rpc.{_check_subject(subject)}"

    def _log_subject(self, topic: str) -> str:
        return f"{self._prefix}.log.{_check_subject(topic)}"

    def _tape_subject(self, feed: str) -> str:
        return f"{self._prefix}.tape.{_check_subject(feed)}"

    def _topic_from_subject(self, subject: str) -> str:
        """The topic a caller asked for, back out of the subject it arrived on."""
        for mid in (".ps.", ".log."):
            needle = f"{self._prefix}{mid}"
            if subject.startswith(needle):
                return subject[len(needle) :]
        return subject

    @property
    def _fanout_stream(self) -> str:
        return _sanitize(f"{self._prefix}_ps")

    @property
    def _log_stream(self) -> str:
        return _sanitize(f"{self._prefix}_log")

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
                # flushed on reconnect. That is right for fan-out, and harmless
                # for a request, which has its own deadline and will fail on
                # that instead.
                pending_size=8 * 1024 * 1024,
            )
            self._owns_connection = True
        self._js = self._nc.jetstream()
        await self._ensure_fanout_stream()

    async def close(self) -> None:
        if self._nc is not None and self._owns_connection:
            # Flush, then close. Not ``drain()``: draining waits for every
            # subscription's handler to finish, and a process shutting down has
            # exactly the subscriptions whose loops are already being cancelled
            # — so it reliably waits out its own timeout and turns a teardown
            # into thirty seconds. The flush is the part worth keeping, because
            # a publish still sitting in the outbound buffer is a message that
            # was accepted and then lost.
            with contextlib.suppress(Exception):
                await self._nc.flush(timeout=_CLOSE_FLUSH_TIMEOUT_S)
            with contextlib.suppress(Exception):
                await self._nc.close()
            self._nc = None
        self._js = None
        self._kv.clear()
        self._kv_status.clear()
        self._state_fields.clear()
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
        """One stream that captures every pub/sub subject this node publishes.

        A catch-all ``{prefix}.ps.>`` rather than a stream per subject family,
        because a family this did not list would not be stored — and the
        families are added by whoever writes a new topic, who has no reason to
        come here. Live ``publish`` does not wait for that capture.
        ``nc.publish`` will not fail if the stream is missing; the catch-all
        is load-bearing for the tail, not for error reporting. The
        per-subject bounds are what keep one stream honest: each subject
        holds its last :data:`FANOUT_MAX_MSGS_PER_SUBJECT` messages, so the
        size follows how many subjects are live rather than how fast the
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
                # NATS refuses to disable message TTLs on a stream that has
                # them, so removing this turns every upgrade against a live
                # server into a failed boot.
                allow_msg_ttl=True,
                allow_direct=True,
            )
        )

    async def _ensure_log_stream(self) -> None:
        """One stream for every ``publish_log`` subject this node writes.

        Separate from fan-out so the ring is the stream's own
        ``max_msgs_per_subject`` rather than a purge after every line. The
        bound is :data:`LOG_MAX_MSGS_PER_SUBJECT`, which covers both the
        default log buffer and the status ring STS and the API ask for.
        """
        await self._ensure_stream(
            js_api.StreamConfig(
                name=self._log_stream,
                subjects=[f"{self._prefix}.log.>"],
                retention=js_api.RetentionPolicy.LIMITS,
                discard=js_api.DiscardPolicy.OLD,
                max_msgs_per_subject=LOG_MAX_MSGS_PER_SUBJECT,
                max_msgs=FANOUT_MAX_MSGS,
                max_age=FANOUT_MAX_AGE_SECONDS,
                allow_msg_ttl=True,
                allow_direct=True,
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
                # this node's before a constant here changed. Updating is right
                # for the difference this is usually about: an operator who
                # raises ``MD_TAPE_MAXLEN`` wants the streams that already exist
                # to follow, and refusing would leave MD unable to record a feed
                # it has ever recorded before until someone deleted them by hand.
                await self._reshape(config)
            self._ensured.add(config.name)

    async def _reshape(self, config: js_api.StreamConfig) -> None:
        """Move a live stream to ``config``, or say what stopped it.

        Not every difference is a widening. A stream that has ``allow_msg_ttl``
        will not give it up — ``update_stream`` answers ``message TTL status can
        not be disabled`` — and there is no version of "apply the config anyway"
        that gets past that.

        What matters is where the refusal surfaces. This runs inside
        :meth:`connect`, so the failure is not one call raising: it is every
        plane failing to start, at once, and only on servers that already hold
        the stream — which is every server except the fresh one a developer
        tests against. A raw ``ServerError`` out of nats-py is a poor thing to
        find at that moment, so the fields that disagree are named here instead.
        """
        try:
            await self.js.update_stream(config)
        except nats.js.errors.APIError as exc:
            differences = await self._stream_differences(config) or ["unknown"]
            raise StreamShapeError(
                f"stream {config.name!r} is live with a shape this build cannot "
                f"move it to: {'; '.join(differences)}. The server refused the "
                f"change rather than applying it, so the stream has to be "
                f"migrated or removed before a node declaring this config can "
                f"start."
            ) from exc

    async def _stream_differences(self, wanted: js_api.StreamConfig) -> list[str]:
        """Fields where the live stream disagrees with what this build declares.

        Only the fields the caller set. A ``StreamConfig`` has thirty-eight of
        them and the server fills most in, so comparing all of them would bury
        the one that matters under defaults nobody wrote down.
        """
        try:
            live = (await self.js.stream_info(str(wanted.name))).config
        except Exception:
            # The message is worth less without this, not worthless: the caller
            # still learns which stream refused and that it refused.
            return []
        out: list[str] = []
        for field in dataclasses.fields(wanted):
            declared = getattr(wanted, field.name)
            if declared is None:
                continue
            current = getattr(live, field.name, None)
            if current != declared:
                out.append(f"{field.name} is {current!r}, declared {declared!r}")
        return out

    async def _ensure_stream_placement(
        self, name: str, cfg: dict[str, Any] | None = None
    ) -> None:
        """Move a live stream onto ``kv_placement_cluster`` if it is not there.

        Called only for KV buckets. The name is the stream's, because the
        JS API is STREAM.UPDATE; fan-out does not use this. A stream has
        one leader, so pinning it to JP makes every TW publisher wait and
        pinning it to TW makes every JP publisher wait. KV is a different
        promise: one ledger, one cluster, and a read that cannot be
        answered locally is a failed read rather than a delayed tick.

        ``cfg`` is the live STREAM.INFO config when the caller already
        has it, so a KV reshape that just fetched does not fetch again.

        A cluster that cannot take the stream is a warning, not a failed
        boot: local and CI have no second cluster, and a production move
        that NATS refuses must not take every plane down with it.
        """
        cluster = self.config.kv_placement_cluster
        if not cluster:
            return
        if cfg is None:
            try:
                raw = await self._js_api(f"STREAM.INFO.{name}")
            except nats.js.errors.NotFoundError:
                return
            cfg = dict(raw["config"])
        else:
            cfg = dict(cfg)
        live_cluster = ((cfg.get("placement") or {}) or {}).get("cluster") or ""
        if live_cluster == cluster:
            return
        cfg["placement"] = {"cluster": cluster}
        try:
            await self._js_api(
                f"STREAM.UPDATE.{name}",
                cfg,
                timeout=_KV_RESHAPE_TIMEOUT_S,
            )
        except nats.js.errors.APIError as exc:
            logger.warning(
                "could not pin %s to cluster %s; leaving %s: %s",
                name,
                cluster,
                live_cluster or "-",
                exc,
            )

    async def _js_api(
        self,
        op: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """One JetStream management request, as the server sent it.

        nats-py's ``StreamConfig`` does not carry ``allow_msg_ttl``. An
        ``update_stream`` built from that type drops the flag, and a stream
        that has it will not give it up (err 10052). Replica and placement
        changes have to go through the raw API so the live config is what
        comes back.
        """
        if self._nc is None:
            raise BrokerNotConnectedError
        body = json.dumps(payload).encode() if payload is not None else b""
        wait = self.config.request_timeout if timeout is None else timeout
        try:
            msg = await self._nc.request(f"$JS.API.{op}", body, timeout=wait)
        except nats.errors.NoRespondersError as exc:
            raise nats.js.errors.ServiceUnavailableError from exc
        resp = json.loads(msg.data)
        if "error" in resp:
            raise nats.js.errors.APIError.from_error(resp["error"])
        return resp

    async def _open_kv(self, name: str) -> nats.js.kv.KeyValue:
        """Create the bucket, or bind it if the live stream already disagrees.

        ``create_key_value`` is ``STREAM.CREATE``. A bucket this process did
        not mint — or one NATS later tagged with ``allow_msg_ttl`` / a
        replica count this build did not send — comes back as 10058, not as
        a handle. Binding is the right answer; reshaping is
        :meth:`_ensure_kv_shape`.
        """
        replicas = max(1, self.config.kv_replicas)
        try:
            return await self.js.create_key_value(
                js_api.KeyValueConfig(bucket=name, history=1, replicas=replicas)
            )
        except nats.js.errors.BadRequestError:
            try:
                return await self.js.key_value(name)
            except nats.js.errors.BucketNotFoundError:
                if replicas <= 1:
                    raise
                return await self.js.create_key_value(
                    js_api.KeyValueConfig(bucket=name, history=1, replicas=1)
                )

    async def _ensure_kv_shape(self, name: str) -> None:
        """Move a live KV stream to the declared replica count and cluster.

        NATS refuses to scale and move in one update (err 10123), so replicas
        change first and placement is a second call. A cluster too small to
        place the declared replica count is a warning, not a failed boot:
        local and CI stay on one node.
        """
        replicas = max(1, self.config.kv_replicas)
        cluster = self.config.kv_placement_cluster
        if replicas <= 1 and not cluster:
            return
        stream = f"KV_{name}"
        try:
            raw = await self._js_api(f"STREAM.INFO.{stream}")
        except nats.js.errors.NotFoundError:
            return
        cfg = dict(raw["config"])
        live_replicas = int(cfg.get("num_replicas") or 1)
        if live_replicas != replicas:
            cfg["num_replicas"] = replicas
            try:
                await self._js_api(
                    f"STREAM.UPDATE.{stream}",
                    cfg,
                    timeout=_KV_RESHAPE_TIMEOUT_S,
                )
            except nats.js.errors.APIError as exc:
                logger.warning(
                    "could not set %s replicas=%s; leaving %s: %s",
                    stream,
                    replicas,
                    live_replicas,
                    exc,
                )
                return
        await self._ensure_stream_placement(stream, cfg=cfg)

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
            bucket = await self._open_kv(name)
            await self._ensure_kv_shape(name)
            self._kv[kind] = bucket
            with contextlib.suppress(Exception):
                self._kv_status[kind] = await bucket.status()
            return bucket

    async def _bucket_status(self, kind: str) -> Any:
        """Cached ``bucket.status()`` — a raft read on a clustered server."""
        cached = self._kv_status.get(kind)
        if cached is not None:
            return cached
        bucket = await self._bucket(kind)
        status = await bucket.status()
        self._kv_status[kind] = status
        return status

    # --- fan-out -----------------------------------------------------------

    async def publish(self, topic: str, raw: str) -> None:
        await self.nc.publish(self._fanout_subject(topic), raw.encode())

    async def subscribe(
        self,
        topics: Sequence[str],
        *,
        stop: asyncio.Event | None,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[tuple[str, str]]:
        subjects = [
            subject
            for t in topics
            for subject in self._subscribe_subjects(t)
        ]
        async for item in self._consume(subjects, stop=stop, ready=ready):
            yield item

    async def psubscribe(
        self, patterns: Sequence[str], *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str]]:
        # Patterns are subjects with wildcards in them, so they pass through
        # ``_check_subject``'s refusal of ``*`` — prefixed by hand instead.
        subjects = [
            subject
            for p in patterns
            for subject in self._subscribe_subjects(p, pattern=True)
        ]
        async for item in self._consume(subjects, stop=stop, ready=None):
            yield item

    def _subscribe_subjects(
        self, topic: str, *, pattern: bool = False
    ) -> tuple[str, ...]:
        fanout = (
            f"{self._prefix}.ps.{topic}" if pattern else self._fanout_subject(topic)
        )
        return (fanout,)

    async def _drain_pending(self) -> None:
        """Write buffered commands to this server, without a PING/PONG.

        ``Client.flush`` is the obvious "wait until the server has it"
        and the wrong one here: it parks a future in ``_pongs``, and a
        cancelled waiter is not removed. The next PONG then completes a
        done future, the read loop dies, and KV / log publishes time out.
        Forcing the pending write is enough for the SUB to leave this
        process; nats-py's own flusher already ignores a cancelled wait.
        """
        await self.nc._flush_pending(force_flush=True)

    async def _consume(
        self,
        subjects: Sequence[str],
        *,
        stop: asyncio.Event | None,
        ready: asyncio.Event | None,
    ) -> AsyncIterator[tuple[str, str]]:
        """One core subscription per subject, merged, until ``stop``.

        Core rather than a JetStream consumer: each subscriber still has
        its own interest, a slow reader cannot cost a fast one anything,
        and a publisher waits for the server it connected to rather than
        the stream's leader. The stream still stores the subject for
        readers; live delivery does not go through it.

        ``nc.subscribe`` does not wait for the SUB to leave ``_pending``.
        Draining the write buffer is what we need — the bytes reach this
        process's server. ``Client.flush`` is a PING/PONG instead, and a
        cancelled waiter leaves its future in ``_pongs``; the next PONG
        then ``set_result``s a done future, kills the read loop, and
        every later ``js.publish`` times out. Session start fires several
        of these pumps at once and a strategy that exits in ``on_start``
        cancels them immediately.

        This is not a wait for gateway interest to the other cluster.
        """
        if not subjects:
            raise ValueError("a subscription needs at least one subject")

        inbound: asyncio.Queue[tuple[str, str]] = asyncio.Queue()

        async def handler(msg: Msg) -> None:
            topic = self._topic_from_subject(msg.subject)
            await inbound.put((topic, msg.data.decode()))

        subs = [
            await self.nc.subscribe(subject, cb=handler) for subject in subjects
        ]
        await self._drain_pending()
        if ready is not None:
            ready.set()
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
        """Publish onto the log stream. The server holds the ring.

        ``maxlen`` above :data:`LOG_MAX_MSGS_PER_SUBJECT` raises: the stream
        has already discarded by then, and a caller quietly given half the
        ring it asked for is worse than one told it asked for too much. A
        smaller ``maxlen`` is the replay cap :meth:`fetch_log_buffer` honours;
        the stream still holds its own bound.

        ``ttl_seconds`` is a per-message TTL; see the contract note on
        :meth:`~mftik.broker.transport.base.BrokerTransport.publish_log`.
        """
        if maxlen > LOG_MAX_MSGS_PER_SUBJECT:
            raise ValueError(
                f"publish_log(maxlen={maxlen}) is above this transport's "
                f"per-subject ceiling of {LOG_MAX_MSGS_PER_SUBJECT}; raise "
                f"LOG_MAX_MSGS_PER_SUBJECT if a ring that long is wanted"
            )
        await self.js.publish(
            self._log_subject(topic),
            raw.encode(),
            headers={js_api.Header.MSG_TTL: str(_ttl_seconds(ttl_seconds))},
        )

    async def fetch_log_buffer(self, topic: str) -> list[str]:
        """Everything the log stream is still holding for ``topic``."""
        subject = self._log_subject(topic)
        held = (await self._subject_counts(self._log_stream, subject)).get(
            subject, 0
        )
        if not held:
            return []
        rows = await self._read(
            self._log_stream,
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

    async def _close_reader(
        self, sub: nats.js.JetStreamContext.PullSubscription
    ) -> None:
        """Retire a read's consumer on the server as well as on the client.

        ``unsubscribe`` on a pull subscription destroys the client's own inboxes
        and stops there — nats-py says so in as many words — so the consumer it
        built goes on existing on the server until ``inactive_threshold`` reaps
        it, which is :data:`_READ_CONSUMER_IDLE_S`. That is a reasonable backstop
        for a process that died mid-read and a poor way to end a read that
        finished.

        Every read here builds a consumer, and one of them is on a hot path: TD
        replaces its whole order book per fill, which reads the book first, so a
        busy account left a new consumer on the state bucket's stream per print
        and carried thirty seconds' worth of them at any moment.
        """
        with contextlib.suppress(Exception):
            await sub.unsubscribe()
        with contextlib.suppress(Exception):
            # Reached for rather than asked for: nats-py has the stream and
            # consumer names right here, and its only public way to the latter is
            # a round trip to the server to be told what we just named.
            await self.js.delete_consumer(sub._stream, sub._consumer)  # noqa: SLF001

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
            await self._close_reader(sub)
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

        So it is re-asked, and for how long is a share of what this caller
        brought — see :data:`_NO_RESPONDERS_SHARE`. A subject that is really
        unserved still fails well inside the timeout, which is what makes core
        request-reply worth having here; a subject whose owner is mid-handover
        gets asked again while the caller still has time to be told yes.
        """
        return await self._ask(
            subject,
            raw,
            request_id=request_id,
            timeout=timeout,
            reask=min(
                max(timeout * _NO_RESPONDERS_SHARE, _NO_RESPONDERS_FLOOR_S),
                _NO_RESPONDERS_CEILING_S,
            ),
        )

    async def _ask(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        timeout: float,
        reask: float,
    ) -> str:
        """One core request, re-asked while nobody is on the subject.

        ``reask`` bounds the re-asking only. Once it is spent, a subject that is
        still empty fails immediately rather than sitting out the rest of
        ``timeout``, because nothing about waiting longer would change the answer.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        give_up_asking = loop.time() + reask
        subject_name = self._rpc_subject(subject)
        payload = raw.encode()
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                msg = await self.nc.request(
                    subject_name, payload, timeout=remaining
                )
            except nats.errors.NoRespondersError:
                if loop.time() + _NO_RESPONDERS_GRACE_S >= give_up_asking:
                    break
                await asyncio.sleep(_NO_RESPONDERS_GRACE_S)
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
        """:meth:`request` without the patience, and that is the point.

        The Redis transport needs a capped, expiring queue here to stop a
        dashboard's probes accumulating against a down instance. Core NATS
        stores nothing anywhere, so there is no queue to cap: a probe nobody
        answers has already left no trace by the time the caller gives up.

        What it does not share is the re-ask budget. An order gains from waiting
        out a handover because the caller wants the order placed; a probe gains
        nothing, because a liveness answer that arrives after the dashboard
        stopped asking tells nobody anything — and "this instance is down" is the
        answer a probe is *for*. So it re-asks only far enough to cover a serve
        loop registering as its own process boots, which is the one case where a
        first no-responders reading is simply wrong.
        """
        return await self._ask(
            subject,
            raw,
            request_id=request_id,
            timeout=timeout,
            reask=_NO_RESPONDERS_FLOOR_S,
        )

    async def serve(
        self, subject: str, *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str | None]]:
        """Core queue subscription. The queue group is what makes a pool.

        A connection that dropped is being reconnected underneath, and a
        plane whose control loop returned stays up with nobody reading its
        requests — so only ``stop`` ends this.
        """
        live_subject = self._rpc_subject(subject)
        group = self._named("group", subject)
        inbound: asyncio.Queue[tuple[str, str | None]] = asyncio.Queue()

        async def handler(msg: Msg) -> None:
            await inbound.put((msg.data.decode(), msg.reply or None))

        live = await self.nc.subscribe(live_subject, queue=group, cb=handler)
        try:
            async for item in _iter_until_stopped(inbound, stop=stop):
                yield item
        finally:
            with contextlib.suppress(Exception):
                await live.unsubscribe()

    async def send_reply(self, inbox: str, raw: str) -> None:
        await self.nc.publish(inbox, raw.encode())

    # --- shared state ------------------------------------------------------

    def _state_key(self, name: str, field: str) -> str:
        return _kv_key(f"{name}.{field}")

    async def _state_stream(self) -> tuple[str, str]:
        """The state bucket's stream, and the prefix its keys hang under.

        Both are needed together by anything reaching past the KV interface to
        the stream underneath, which is what a purge that leaves no marker has to
        do.
        """
        status = await self._bucket_status("state")
        stream = status.stream_info.config.name
        assert stream is not None
        return stream, f"$KV.{status.bucket}."

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
            known = self._state_fields.get(name)
            if known is not None:
                known.update(values)

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
            before = self._state_fields.get(name)
            if before is None:
                before = set(await self._all_fields(name))
            await self._put_fields(name, values)
            stale = before - set(values)
            if stale:
                await self._drop_fields(name, sorted(stale))
            self._state_fields[name] = set(values)

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
        """Every live key under ``pattern`` with its value, in one pass.

        Complete or an exception, never quietly short. The count the batch is
        sized from is the stream's own, so a read that comes up against it has
        either lost a race with a writer or not finished, and the two are told
        apart rather than both answered with a smaller dict — this is how a
        strategy reads its open orders, and a book missing rows looks exactly
        like a book that small.
        """
        status = self._kv_status.get("state")
        if status is None:
            status = await bucket.status()
            self._kv_status["state"] = status
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
            # Counted apart from ``rows`` because a marker is delivered and then
            # dropped: it is one of the subjects above, so the accounting has to
            # see it even though the answer does not. ``_drop_fields`` purges
            # the marker after the delete; an older build's leftovers remain.
            delivered = 0
            expected = len(subjects)
            while delivered < expected:
                try:
                    msgs = await sub.fetch(
                        batch=min(expected - delivered, _READ_BATCH),
                        timeout=_READ_TIMEOUT_S,
                    )
                except (nats.errors.TimeoutError, TimeoutError):
                    msgs = []
                if not msgs:
                    # Either a key went while this was reading, which makes the
                    # smaller answer the current one, or the read did not finish.
                    # The stream knows which.
                    if len(await self._subject_counts(stream, subject)) <= delivered:
                        break
                    raise StateReadIncompleteError(
                        f"read {delivered} of {expected} keys under {pattern}"
                    )
                delivered += len(msgs)
                for msg in msgs:
                    # A delete or a purge leaves a marker under the key, which is
                    # how a watcher learns the key went. To a reader it is simply
                    # absent.
                    if (msg.headers or {}).get("KV-Operation") in ("DEL", "PURGE"):
                        continue
                    rows[msg.subject[len(head) :]] = msg.data.decode()
        finally:
            await self._close_reader(sub)
        return rows

    async def state_drop(self, name: str, fields: Sequence[str]) -> int:
        if not fields:
            return 0
        async with self._state_lock(name):
            return await self._drop_fields(name, fields)

    async def _drop_fields(self, name: str, fields: Sequence[str]) -> int:
        """Remove these fields so a watcher sees each one go.

        KV delete writes a marker — that is how :meth:`state_watch` learns a
        field left. The marker is then purged from the bucket's stream so
        :meth:`_kv_scan` does not transfer one extra subject per order this
        account has ever worked. A key that is already gone is not deleted
        again, or a repeated drop would mint a fresh marker.
        """
        bucket = await self._bucket("state")
        dropped_keys: list[str] = []
        for field in fields:
            key = self._state_key(name, field)
            if not await self._state_field_exists(bucket, key):
                # A leftover DEL marker still occupies a subject. Sweep it
                # without minting another — nats-py ``delete`` never raises
                # on a missing key, so an unchecked call would grow the
                # bucket by one marker per historical order.
                await self._purge_state_key(key)
                continue
            await bucket.delete(key)
            dropped_keys.append(key)
        known = self._state_fields.get(name)
        if known is not None:
            known.difference_update(fields)
        for key in dropped_keys:
            await self._purge_state_key(key)
        return len(dropped_keys)

    async def _state_field_exists(
        self, bucket: nats.js.kv.KeyValue, key: str
    ) -> bool:
        try:
            entry = await bucket.get(key)
        except (nats.js.errors.KeyNotFoundError, nats.js.errors.KeyDeletedError):
            return False
        if entry is None or entry.value is None:
            return False
        operation = getattr(entry, "operation", None)
        op = getattr(operation, "value", operation)
        return op not in ("DEL", "PURGE")

    async def _purge_state_key(self, key: str) -> None:
        """Remove the delete marker so the bucket does not grow without bound."""
        stream, head = await self._state_stream()
        with contextlib.suppress(Exception):
            await self.js.purge_stream(stream, subject=f"{head}{key}")

    async def state_clear(self, names: Sequence[str]) -> None:
        """Drop every field of these names so watchers see each deletion."""
        for name in names:
            async with self._state_lock(name):
                fields = list(await self._all_fields(name))
                if fields:
                    await self._drop_fields(name, fields)
                self._state_fields.pop(name, None)

    async def state_watch(
        self, name: str, *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str | None]]:
        """Yield ``(field, value)`` for ``name``. ``None`` value means deleted.

        One watch. The caller reseeds from :meth:`state_all` and opens another
        if this ends — that is how a projection resynchronises after a
        reconnect, and how a purged delete marker is not mistaken for a live
        field.
        """
        prefix = _kv_key(f"{name}.")
        watcher = None
        try:
            bucket = await self._bucket("state")
            watcher = await bucket.watch(f"{prefix}>")
            async for update in watcher:
                if stop is not None and stop.is_set():
                    return
                if update is None or not update.key:
                    continue
                key = update.key
                field = key[len(prefix) :] if key.startswith(prefix) else key
                operation = getattr(update, "operation", None)
                op = getattr(operation, "value", operation)
                if op in ("DEL", "PURGE"):
                    yield field, None
                    continue
                if update.value is None:
                    continue
                yield field, update.value.decode()
        finally:
            if watcher is not None:
                with contextlib.suppress(Exception):
                    await watcher.stop()

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
        status = await self._bucket_status("lease")
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
        status = await self._bucket_status("lease")
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
            await self._close_reader(sub)
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
        del ttl_seconds
        merged = {
            **await self.tape_coverage(feed),
            **{k: str(v) for k, v in values.items()},
        }
        await self._tape_coverage_write(feed, merged)

    async def _tape_coverage_write(
        self, feed: str, record: Mapping[str, str]
    ) -> None:
        """Write this feed's whole coverage record. Durable — no per-message TTL.

        A recording longer than a TTL used to lose its own description while
        live. The tape stream already expires prints via ``max_age``; coverage
        is a fact about those prints and stays until the next mark overwrites
        it or the bucket is dropped.
        """
        bucket = await self._bucket("tapecov")
        await bucket.put(_kv_key(feed), json.dumps(dict(record)).encode())


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


class _Stopped:
    """The stop event, in a shape a queue can carry.

    One instance, :data:`_STOPPED`, and it is recognised by identity: no message
    can be this object, whatever its topic and payload turn out to be.
    """

    __slots__ = ()


_STOPPED = _Stopped()


async def _tap_stop(stop: asyncio.Event, inbound: asyncio.Queue[Any]) -> None:
    """Turn the stop event into the last item on ``inbound``."""
    await stop.wait()
    inbound.put_nowait(_STOPPED)


async def _iter_until_stopped(
    inbound: asyncio.Queue[tuple[str, str | None]] | asyncio.Queue[tuple[str, str]],
    *,
    stop: asyncio.Event | None,
) -> AsyncIterator:
    """Yield from ``inbound`` until ``stop`` is set.

    Stopping the moment it is told to, rather than waiting out a poll, is what
    makes a NATS teardown quick. The Redis transport cannot do it — a blocking
    pop is not cancellable without losing what it was about to return — and
    waiting out one poll per teardown is what the whole suite used to pay.

    The obvious way to get that is to race ``inbound.get()`` against
    ``stop.wait()`` under :func:`asyncio.wait`, and it is wrong twice over.
    Racing needs the read wrapped in a task, and a task is not something the
    caller's cancellation reaches: ``asyncio.wait`` drops its own callbacks when
    it is cancelled and leaves what it was waiting on alone, so a stopped
    session left that read pending until the collector found it and asyncio
    logged ``Task was destroyed but it is pending`` (#81). And cancellation can
    land in the instant *after* the read has taken a message, where cancelling
    is a no-op and the message is already out of the queue and in a local with
    nowhere left to go.

    So the stop event arrives through the queue rather than beside it. There is
    one waiter, this generator awaits it directly instead of a task doing it,
    and both problems become asyncio's own contract: a cancellation reaches the
    read, and a cancelled ``Queue.get`` leaves what it was about to take in the
    queue instead of holding it. Nothing is dropped here because nothing is ever
    held here.

    Handover order mostly falls out of it too — the sentinel goes to the tail,
    so everything queued ahead of it is yielded first.

    The price is one task, and :func:`_tap_stop` holds no message — which is the
    whole reason to prefer it to one that does.
    """
    if stop is None:
        while True:
            yield await inbound.get()

    tap = asyncio.create_task(_tap_stop(stop, inbound))
    try:
        while True:
            item = await inbound.get()
            if item is _STOPPED:
                break
            yield item
        while not inbound.empty():
            yield inbound.get_nowait()
    finally:
        tap.cancel()
        # Awaited, not merely cancelled, so a teardown finishes quiet rather
        # than eventually: a cancelled task is not yet a done task, and a caller
        # that checks would otherwise have to guess how long to wait first.
        with contextlib.suppress(asyncio.CancelledError):
            await tap
