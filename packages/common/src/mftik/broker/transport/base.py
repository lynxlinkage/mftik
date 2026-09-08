"""What a transport owes the broker.

:class:`~mftik.broker.client.Broker` is the vocabulary a plane may speak, and
until this split it was also the Redis implementation of that vocabulary. The
two are separated here so a second transport is a class rather than a fork:
everything a plane says arrives at one of the methods below, and nothing else
about the store underneath is visible above this line.

The seam sits at *serialized envelopes*, not at store primitives. That is the
whole design decision in this file, and it is worth saying why, because the
obvious alternative reads better and is wrong.

A transport of primitives — push, pop, hash-set, expire — would have let the
broker keep every method it has and swap only the bottom. But those primitives
are Redis' own: a list with a blocking pop, a hash of fields, a key with a
TTL. NATS has none of them in that shape, so each would have been emulated
badly, and the emulation would have been the part that broke. The families
below are instead named for what a caller *wants*, which both stores can
answer in their own idiom — a lease is "may I be the one who runs this", not
"``SET NX PX``".

What stays above this line is everything that is not about the store: envelope
encoding, the continuity arithmetic in :meth:`Broker.tape_mark_recording`, the
gap codec, :class:`~mftik.broker.request.IncomingRequest` and
:class:`~mftik.broker.link.LeasedSessionLink`. None of that gets a second
implementation, so none of it is a transport's business.

Strings, not models, cross the seam. The broker already serialized at exactly
this boundary — ``envelope.to_json()`` on the way out, ``from_json`` on the way
back — so putting the line here costs no extra parse and keeps pydantic out of
every transport.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

#: What a lease stores when its holder has no name worth writing down. A
#: session liveness key answers "is anybody still here", never "who", so the
#: value is a placeholder and :meth:`BrokerTransport.lease_owner` on one of
#: those tells a reader nothing it did not already know from the lease
#: existing at all.
LEASE_ANONYMOUS = "1"


def redacted_url(url: str) -> str:
    """``url`` with its password replaced, for logging.

    Every store this speaks to takes its credential inline in the URL, and
    every service logs :meth:`BrokerTransport.describe` on every boot — so a
    password left in one lands in ``docker logs`` for the whole fleet and in
    anything those logs are shipped to. It lives here rather than in a
    transport because it is how the ``describe`` contract below is kept, and
    the next transport should find it already written.

    Parsed rather than pattern-matched: a password may contain ``@`` and ``:``,
    so splitting on either finds the wrong one and prints the rest. Anything
    that will not parse returns a placeholder — falling back to the original
    would leak exactly the string this exists to hide.
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


class BrokerTransport(ABC):
    """One store, answering the broker's vocabulary.

    Implementations live beside this file and are built through
    :func:`mftik.broker.transport.build`. One exists.

    Every method here is called by :class:`~mftik.broker.client.Broker` and by
    nothing else. A domain that reached one directly would be going around the
    broker, which is what
    ``packages/common/tests/test_broker_is_the_only_transport.py`` fails on.
    """

    # --- lifecycle ---------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """Open the connection, and fail here if the store is unreachable.

        Called once per process, before anything else. A transport that defers
        its first round trip would move the failure into whichever plane
        happened to speak first, which is how a bad URL becomes a market-data
        stall rather than a startup error.
        """

    @abstractmethod
    async def close(self) -> None:
        """Release the connection. Safe to call twice, and on a failed connect."""

    @abstractmethod
    def describe(self) -> str:
        """One line naming where this is connected, for the startup log.

        Credentials must already be redacted: every service logs this on every
        boot, so anything left in it lands in ``docker logs`` for the whole
        fleet and in whatever ships those logs onward.
        """

    # --- fan-out -----------------------------------------------------------

    @abstractmethod
    async def publish(self, topic: str, raw: str) -> None:
        """Hand one message to every current subscriber of ``topic``.

        Best effort, and deliberately so: a message published while nobody is
        subscribed is gone. Callers that cannot accept that use
        :meth:`publish_log` or request-reply instead, and the two topics that
        do are the ones this promise is written down for.

        Nothing comes back. Redis answers with a delivery count, which no
        caller ever read and which a subject-based transport cannot produce.
        """

    @abstractmethod
    def subscribe(
        self, topics: Sequence[str], *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str]]:
        """Yield ``(topic, raw)`` from ``topics`` until ``stop`` is set.

        Yielding the topic even for the single-topic case is what lets
        :meth:`psubscribe` share the plumbing.
        """

    @abstractmethod
    def psubscribe(
        self, patterns: Sequence[str], *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str]]:
        """Yield ``(topic, raw)`` for topics matching ``patterns``.

        Patterns use one wildcard per segment — ``log.*.*``, never ``log.*``.
        Redis globs the whole channel name and would accept the short form;
        a transport that matches per segment reads it as a two-segment subject
        and silently delivers nothing. See the note above
        :meth:`~mftik.protocol.Topics.log_pattern`.
        """

    # --- fan-out with a tail -----------------------------------------------

    @abstractmethod
    async def publish_log(
        self, topic: str, raw: str, *, maxlen: int, ttl_seconds: int
    ) -> None:
        """Publish onto the log stream for live subscribers and late replay.

        For logs, where the UI socket opens after the deploy it wants to watch
        and still expects the lines it missed. Live subscribers see this exactly
        as they would a :meth:`publish`; the buffer is extra, not instead.

        ``maxlen`` above the log stream's per-subject cap raises rather than
        keeping fewer: a caller reading back half of what it asked for has no
        way to tell that from a quiet hour. A smaller ``maxlen`` is the replay
        cap :meth:`fetch_log_buffer` honours; the stream holds its own ring.

        ``ttl_seconds`` is a per-message TTL, so a line expires on its own
        clock rather than the buffer expiring as a whole. A topic that has
        gone quiet still drops, which is the property callers pass it for.
        """

    @abstractmethod
    async def fetch_log_buffer(self, topic: str) -> list[str]:
        """What the log stream is still holding for ``topic``, oldest first.

        The caller trims: :meth:`Broker.fetch_log_buffer` applies ``maxlen``.
        """

    # --- request-reply -----------------------------------------------------

    @abstractmethod
    def reply_inbox(self, request_id: str) -> str | None:
        """Where a reply to ``request_id`` should be addressed, if in-band.

        The two transports disagree about where a reply address lives, and this
        is how the broker avoids caring. Redis has no reply channel of its own,
        so the address is a key the requester invents and writes into the
        envelope's ``reply_to`` — in-band, because the serving process sees
        nothing but the JSON. NATS carries a reply subject beside the message
        and answers ``None`` here.

        Either way :meth:`serve` hands back the address it found, and the
        broker stamps it onto the envelope before a handler sees it, so
        ``req.envelope.reply_to`` is populated on both. Handlers turn on that
        field — ``mftik_td.backfill.session`` checks it before spending minutes
        on an answer nobody is waiting for.
        """

    @abstractmethod
    async def request(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        inbox: str | None,
        timeout: float,
    ) -> str:
        """Send ``raw`` and return the one reply, or time out.

        Raises :class:`~mftik.broker.errors.RequestTimeoutError` when no reply
        arrives inside ``timeout``, and that error is the caller's answer far
        more often than it is a bug: it is how the control plane reports a
        plane that is not there.

        ``inbox`` is whatever :meth:`reply_inbox` returned for this id, already
        stamped into ``raw``. A transport that answered ``None`` there ignores
        it.
        """

    @abstractmethod
    async def probe(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        inbox: str | None,
        timeout: float,
    ) -> str:
        """Ask whether anybody serves ``subject``, leaving nothing behind.

        :meth:`request` re-asks through a boot race. A probe does not: "down"
        is the answer it exists to collect, and a dashboard polling a down
        instance must not wait out a handover that will not come.

        The reply path is :meth:`request`'s. Core NATS stores nothing, so there
        is no queue to leave behind.
        """

    @abstractmethod
    def serve(
        self, subject: str, *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str | None]]:
        """Yield ``(raw, reply_inbox)`` for work on ``subject`` until ``stop``.

        Several processes of one plane serve the same subject and share the
        work: whoever is free takes the next request. That is what makes the
        anycast subjects — ``sts``, ``md``, ``sym``, ``paper`` — a pool rather
        than a race, and it is why the per-session and per-account subjects
        exist at all, since there the only correct consumer is the one holding
        the session.

        Only ``stop`` may end this. Not an unreachable store and not a message
        that will not parse, because this generator *is* a plane's control
        plane: when it returns the process stays up, the sessions keep trading,
        and every request piles up unread. That is the most expensive way a
        service can fail and the quietest.
        """

    @abstractmethod
    async def send_reply(self, inbox: str, raw: str) -> None:
        """Answer a request on the ``inbox`` :meth:`serve` handed over."""

    # --- shared state ------------------------------------------------------
    #
    # Fan-out says something changed; this holds what it changed *to*. A late
    # subscriber, a restarted process and a strategy that missed a message all
    # read the same current answer here, which is what makes "the writer's
    # state and the reader's agree" true by construction rather than by both
    # sides keeping a copy in step.

    @abstractmethod
    async def state_put_many(self, name: str, values: Mapping[str, str]) -> None:
        """Write fields of ``name``, leaving the others alone.

        One field lands whole or not at all on both transports, so a reader never
        sees half of one value. The *set* is not promised to land together:
        Redis writes them in a single ``HSET`` and NATS writes a key per field,
        so a reader there can catch a two-asset ledger write with one asset
        updated and one not. Each value it sees is a real value the writer wrote;
        what it may not get is a snapshot of the same instant.

        No method here promises a snapshot, then — :meth:`state_replace` is
        explicit that seeing old and new at once is allowed. A caller that needs
        two numbers to move together has to put them in one field, where the
        value is the unit both transports write whole.
        """

    @abstractmethod
    async def state_replace(self, name: str, values: Mapping[str, str]) -> None:
        """Make ``name`` exactly ``values`` — the reconciliation path.

        A reader must never observe the empty gap between the old contents and
        the new. Seeing both at once is allowed; seeing neither is not, because
        a strategy that reads an empty ledger mid-recon concludes it has no
        balance.
        """

    @abstractmethod
    async def state_get(self, name: str, field: str) -> str | None: ...

    @abstractmethod
    async def state_all(self, name: str) -> dict[str, str]: ...

    @abstractmethod
    async def state_drop(self, name: str, fields: Sequence[str]) -> int:
        """Remove ``fields``. Returns how many were there."""

    @abstractmethod
    async def state_clear(self, names: Sequence[str]) -> None:
        """Drop whole states — call when their owner goes away.

        State that outlives its writer is worse than none: a reader cannot tell
        a stale answer from a current one.
        """

    @abstractmethod
    def state_watch(
        self, name: str, *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str | None]]:
        """Yield ``(field, raw)`` as ``name`` changes. ``None`` raw is a delete.

        A watch that dies is restarted and re-delivers the current values,
        which is how a projection resynchronises after a reconnect.
        """

    # --- leases ------------------------------------------------------------
    #
    # A lease is a fact about *now* that its holder may never get to retract:
    # a process running a session, holding an account, walking an account's
    # history. The states above are deleted by their owner, which covers every
    # ending the owner is around to see and not the one that matters here —
    # SIGKILL, OOM, the machine going away — after which a fact with no expiry
    # is a session the UI shows as running that nobody can stop.
    #
    # So a lease always expires, and its holder renews it while it lives.
    #
    # Three of them decide *who* holds a resource, and that is why they are
    # methods here rather than a read and a write composed by each caller: a
    # decision that takes two round trips has a race in the middle, and the
    # only place that race can be closed is in the transport.

    @abstractmethod
    async def lease_put(
        self, name: str, *, ttl: float, owner: str = LEASE_ANONYMOUS
    ) -> None:
        """Write a lease expiring ``ttl`` seconds from now, unconditionally.

        States a fact rather than asking a question, which is right for a
        holder renewing its own and wrong for anything deciding who the holder
        is. A heartbeat that checked first would stop renewing the moment its
        own lease lapsed, when re-taking it is exactly what it wants.
        """

    @abstractmethod
    async def lease_take(
        self, name: str, *, ttl: float, owner: str = LEASE_ANONYMOUS
    ) -> bool:
        """Take ``name`` only if nobody holds it. Whether it was taken.

        The atomic half of a claim, and the only part that cannot be assembled
        from a read and a write. Several processes of a plane come up together
        and each asks for the same session; without the test being part of the
        write they are all told yes and all run it.
        """

    @abstractmethod
    async def lease_owner(self, name: str) -> str | None:
        """Who holds ``name``, or ``None``.

        Answers :data:`LEASE_ANONYMOUS` for a lease taken without a name, which
        tells a reader nothing beyond "somebody".
        """

    @abstractmethod
    async def lease_hold(self, name: str, *, owner: str, ttl: float) -> bool:
        """Extend ``name`` if ``owner`` still holds it. ``False`` if not.

        Conditional, and the condition is the point. A holder whose lease
        lapsed while a rival took it must not extend the rival's or overwrite
        it — the first is a caller believing it holds a resource it lost, the
        second is two processes trading one account.

        A missing lease is never re-created here. Whoever let one expire goes
        back through :meth:`lease_take`, where a rival gets to say no.
        """

    @abstractmethod
    async def lease_release(self, name: str, *, owner: str) -> bool:
        """Give up ``name``, if it is still ``owner``'s to give up.

        Conditional for :meth:`lease_hold`'s reason: a process shutting down
        may already have lost its lease to the one that replaced it, and
        deleting a stranger's claim on the way out hands the resource to a
        third while the second still believes it holds it.

        Releasing rather than waiting out the TTL is what keeps a redeploy from
        looking like an outage nobody caused.
        """

    @abstractmethod
    async def lease_drop(self, name: str) -> None:
        """Delete ``name`` whoever holds it. Safe when there is none.

        The counterpart to an unnamed holder: "is it still mine" has no answer
        to check when nobody signed. Callers that named themselves want
        :meth:`lease_release`.
        """

    # --- counters ----------------------------------------------------------

    @abstractmethod
    async def counter_next(self, name: str) -> int:
        """Increment ``name`` and return the value that came back.

        Shared rather than process-local because the callers are competing
        consumers: several processes of a plane serve one subject, so a counter
        each would hand two of them the same number.

        Monotonic and unbounded. Folding it into a range is the caller's job,
        and how wide that range is decides how long a value takes to repeat.
        """

    # --- recorded tape -----------------------------------------------------
    #
    # A feed's own history, kept so a strategy starting later can warm up on
    # what it missed. Two bounds, meaning different things: ``maxlen`` is the
    # memory fuse, and the trim is the policy. Whichever binds first is what a
    # reader gets, and the coverage record is how it finds out which.

    @abstractmethod
    async def tape_append(
        self,
        feed: str,
        fields: Mapping[str, str],
        *,
        maxlen: int,
        ttl_seconds: int,
        recorded_ms: int | None = None,
    ) -> None:
        """Append one record, holding ``feed`` to ``maxlen`` records.

        The record's stamp is a clock, not the venue's timestamp. Event time
        rides on the record as a field, because a venue tape is not strictly
        monotonic and one late print out of a million must not be able to end a
        recording.

        ``recorded_ms`` names that stamp, and ``None`` — which is what
        production passes — means "use your own clock". It is here because the
        stamp is otherwise unreachable from outside: it is assigned by Redis or
        by the NATS server at write time, and a test about a *gap* in a tape
        needs records further apart than it is willing to sleep for.

        The tape stream expires prints via ``max_age``. Coverage is a durable
        KV record and is not renewed on append.
        """

    @abstractmethod
    async def tape_tail(
        self, feed: str, *, count: int
    ) -> list[tuple[int, dict[str, str]]]:
        """The newest ``count`` records as ``(recorded_ms, fields)``, oldest first.

        Oldest to newest because a warm-up replays forward. The *newest*
        ``count`` because warming up means catching up to now, and a tape held
        by two independent bounds contains an unknown number of records, so
        "the first N" is not a window anybody asked for.

        ``recorded_ms`` is milliseconds on the transport's clock — the same
        clock the continuity mark below is measured against. Readers used to
        take a Redis stream id apart themselves to get it, which put ``<ms>-<seq>``
        in the strategy SDK and in half a dozen tests; parsing it is this
        method's job now.
        """

    @abstractmethod
    async def tape_trim_before(self, feed: str, *, min_id_ms: int) -> int:
        """Drop records stamped before ``min_id_ms``. How many went.

        The retention *policy*, called on a timer against every feed still
        being recorded. A transport whose retention is declarative rather than
        swept may have nothing to do here and should say so, but it must then
        hold the same window some other way: a reader that gets more history
        than the operator configured is a silent policy change, not a bonus.
        """

    @abstractmethod
    async def tape_coverage(self, feed: str) -> dict[str, str]:
        """What ``feed`` currently covers, or ``{}`` if never recorded.

        The fields are the broker's, not a transport's: ``continuous_since_ms``,
        ``recording``, ``stopped_ms``, ``gaps``. A transport stores them and
        reads them back; what they mean is
        :meth:`Broker.tape_mark_recording`'s business.
        """

    @abstractmethod
    async def tape_coverage_put(
        self, feed: str, values: Mapping[str, str], *, ttl_seconds: int
    ) -> None:
        """Write coverage fields for ``feed``, leaving the others alone.

        ``ttl_seconds`` is accepted for the caller's clock and ignored: coverage
        is durable. A feed that goes quiet keeps its description until the next
        mark overwrites it.
        """
