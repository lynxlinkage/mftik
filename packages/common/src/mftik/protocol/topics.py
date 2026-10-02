"""Stream and channel name helpers for the MFTIK broker protocol."""

from __future__ import annotations

import hashlib

from mftik.exchange.tickers import UniversalTicker


def atom_hash(atom_id: str) -> str:
    """Stable subject token for an ``atom_id`` (§6.1).

    SHA-256 of the UTF-8 ``atom_id``, hex. One NATS token: hex contains
    no ``.``. The same ``atom_id`` hashes the same in every process,
    which is what lets a session subscribe without asking MD which
    subject an atom landed on. MD still keeps the hash-to-atom table;
    the hash is not truncated, so two ``atom_id`` strings do not share
    a subject.

    ``atom_id`` is ``venue:endpoint:channel``
    (:class:`mftik.exchange.atoms.Atom`). The channel is why this exists:
    it is the venue's own subscribe parameter and it contains ``.``.
    """
    if not atom_id:
        raise ValueError("atom_id is empty")
    return hashlib.sha256(atom_id.encode("utf-8")).hexdigest()


class Topics:
    # Request-reply subjects (control plane)
    #
    # These are the *anycast* subjects: a plane's shared pool, taken by
    # whichever of its processes gets there first. That is the right shape for
    # work nobody has addressed — a ``strategy.yml`` that names no instance —
    # and the wrong shape for work that has been. The named form is
    # :meth:`td`, :meth:`sts` and :meth:`md` below.
    #
    # ``SYM`` and ``PAPER`` have no named form. Neither plane is instanced:
    # SYM is off the hot path behind ``SymbolClient``'s cache, and one shared
    # book is the whole point of paper.
    #: ``TD`` is deliberately absent. Everything that reaches TD carries an
    #: ``api_id`` and an ``api_id`` names its instance, so there is no
    #: unaddressed TD work — see :meth:`td`.
    STS = "sts"
    MD = "md"
    SYM = "sym"
    PAPER = "paper"

    @staticmethod
    def td(instance: str) -> str:
        """Control-plane subject one named TD answers on.

        The anycast :attr:`TD` is what a caller uses when it has not decided
        which process should take the work. Attach has decided — the ``apis``
        row names the instance allowed to use that credential — so it comes
        here instead.

        Reads like the fan-out topics next door (:meth:`td_global` is
        ``td.{api_id}.global``) and is not one: a transport puts request-reply
        in a namespace of its own — a ``:rpc:`` key prefix under Redis, a
        ``.rpc.`` subject root under NATS — while a fan-out topic is the bare
        name. The two have never overlapped and this does not change that.
        """
        return f"td.{instance}"

    @staticmethod
    def sts(instance: str) -> str:
        """Control-plane subject one named STS answers on."""
        return f"sts.{instance}"

    @staticmethod
    def md(instance: str) -> str:
        """Control-plane subject one named MD answers on.

        Not :meth:`md_session`, which is ``md.{session_id}`` and a fan-out
        topic. Same reasoning as :meth:`td`: an rpc subject lives in the
        transport's own request-reply namespace, a topic is the bare name, and
        the two have never shared a keyspace.
        """
        return f"md.{instance}"

    @staticmethod
    def health(domain: str, instance: str) -> str:
        """Request-reply subject one instance answers liveness on.

        Separate from the plane's work subject, and not because it is tidier.
        Every other subject in this class is a live request: nobody serving
        it is an immediate error, and the backstop lives outside the broker
        — a lease, a cursor, the next cron tick. A health check is the one
        request where even a late answer is worthless. An answer that arrives
        after the question stopped being asked tells nobody anything, and a
        dashboard polling an instance that is down should learn that at once
        (see :meth:`Broker.probe`).
        """
        return f"health.{domain}.{instance}"

    @staticmethod
    def paper_orders(api_key: str) -> str:
        return f"paper.{api_key}.orders"

    @staticmethod
    def paper_fills(api_key: str) -> str:
        return f"paper.{api_key}.fills"

    @staticmethod
    def paper_balances(api_key: str) -> str:
        return f"paper.{api_key}.balances"

    @staticmethod
    def paper_order_book(symbol: str) -> str:
        """Paper engine → MD public order-book stream."""
        return f"paper.public.orderbook.{symbol}"

    # Legacy / reserved command subjects
    CMD_TRADING = "cmd.trading"
    CMD_STRATEGY = "cmd.strategy"
    CMD_MARKET_DATA = "cmd.market_data"

    # Heartbeats / control
    HEARTBEAT = "sys.heartbeat"

    @staticmethod
    def log_sts(session_id: str) -> str:
        """Pub/sub channel for STS session logs (``/ws/sts/{session_id}``)."""
        return f"log.sts.{session_id}"

    @staticmethod
    def log_td(api_id: int) -> str:
        """Pub/sub channel for TD account logs (``/ws/td/{api_id}``)."""
        return f"log.td.{api_id}"

    @staticmethod
    def log_md(venue: str) -> str:
        """Pub/sub channel for MD venue logs (``/ws/md/{venue}``)."""
        return f"log.md.{venue}"

    @staticmethod
    def log_session(session_id: str) -> str:
        """Deprecated alias for :meth:`log_sts`."""
        return Topics.log_sts(session_id)

    # Subscription patterns
    #
    # One ``*`` per segment, and never a ``*`` left to span the separators
    # itself. The two transports do not read a pattern the same way: Redis
    # globs the whole channel name, so ``log.*`` matches ``log.sts.s-1``, while
    # NATS matches per segment and reads ``log.*`` as a two-segment subject that
    # nothing publishes to. ``log.*.*`` means the same thing to both.
    #
    # Kept here rather than at the two call sites because a pattern that
    # matches nothing fails silently: the log persister and the alert matcher
    # would simply stop seeing lines, with every publisher still publishing
    # and nothing in any log to say so. ``test_topic_patterns.py`` checks the
    # rule against every ``*_pattern`` on this class.

    @staticmethod
    def log_pattern() -> str:
        """Every log channel: :meth:`log_sts`, :meth:`log_td`, :meth:`log_md`."""
        return "log.*.*"

    @staticmethod
    def td_global_pattern() -> str:
        """Every trading account's private fan-out — see :meth:`td_global`."""
        return "td.*.global"

    @staticmethod
    def status_sts() -> str:
        """STS session state changes, every session (``/ws/status/sts``).

        One channel rather than one per session: the strategies list renders
        every session at once and does not know the id of a session that has
        not been deployed yet, so per-session channels would mean N sockets
        and a blind spot for anything new.
        """
        return "status.sts"

    @staticmethod
    def td_global(api_id: int) -> str:
        """TD → STS fan-out for a trading account (refcount 0→1)."""
        return f"td.{api_id}.global"

    @staticmethod
    def td_session(api_id: int, session_id: str) -> str:
        """TD → STS per-session channel (events + lease ACK)."""
        return f"td.{api_id}.{session_id}"

    @staticmethod
    def sts_control(session_id: str) -> str:
        """Request-reply subject for acting on **one** running session.

        ``sts.ctl.{session_id}`` (§3.1, §5.1). The previous spelling was
        ``sts.control.{session_id}``. Stop, fail and status are answered
        by the session worker, so on the shared ``sts`` subject a second
        STS could take a stop for a session it does not hold and answer
        ``not_found``. ``serve`` is a competing consumer; one subject per
        session makes the worker the only consumer there is.

        Only while the session is live. A request for one that has ended
        waits in the list — callers check the row before sending, and
        answer from the table when the table already knows.
        """
        return f"sts.ctl.{session_id}"

    @staticmethod
    def sts_status(session_id: str) -> str:
        """Per-session status snapshots (§3.3, §5.2).

        ``sts.status.{session_id}``. The aggregate :meth:`status_sts`
        channel stays, because the UI socket already subscribes to every
        session on one subject. This is the v2 progress channel.
        """
        return f"sts.status.{session_id}"

    @staticmethod
    def sts_status_pattern() -> str:
        """Every :meth:`sts_status` channel."""
        return "sts.status.*"

    @staticmethod
    def sts_td_session(session_id: str) -> str:
        """STS → TD per-session channel (lease heartbeat + cmds)."""
        return f"sts.td.{session_id}"

    @staticmethod
    def sts_md_session(session_id: str) -> str:
        """STS → MD per-session channel (lease heartbeat + subscribe/detach)."""
        return f"sts.md.{session_id}"

    @staticmethod
    def md_session(session_id: str) -> str:
        """MD → STS per-session channel.

        Kept as a subject-name generator. Market data moved to
        :meth:`md_atom`; nothing in the new protocol publishes here.
        """
        return f"md.{session_id}"

    @staticmethod
    def md_atom(venue: str, digest: str) -> str:
        """``md.a.{venue}.{hash}`` (§6.1, §8.3).

        ``digest`` is :func:`atom_hash` of an ``atom_id``. Both arguments
        are one subject token. A ``.`` in either would split the subject,
        which is the reason the channel is hashed in the first place.
        """
        if not venue or "." in venue or not digest or "." in digest:
            raise ValueError(
                "md.a subject tokens must be non-empty and contain no '.': "
                f"venue={venue!r} hash={digest!r}"
            )
        return f"md.a.{venue}.{digest}"

    @staticmethod
    def md_atom_pattern() -> str:
        """Every :meth:`md_atom` subject."""
        return "md.a.*.*"

    @staticmethod
    def atom_subject(atom_id: str) -> str:
        """Subject one atom is published on.

        The venue is the ``atom_id``'s first segment
        (``venue:endpoint:channel``,
        :class:`mftik.exchange.atoms.Atom`). The whole id is hashed,
        because the channel contains ``.`` (§6.1).
        """
        venue, separator, rest = atom_id.partition(":")
        if not separator or not venue or not rest or "." in venue:
            raise ValueError(
                f"invalid atom_id {atom_id!r}; expected venue:endpoint:channel"
            )
        return Topics.md_atom(venue, atom_hash(atom_id))

    @staticmethod
    def md_worker(instance: str, worker_id: str) -> str:
        """``md.w.{instance}.{worker_id}`` (§5.6).

        The plan marks the shape provisional; this is that spelling.
        ``worker_id`` is one subject token (``md/conn/Deribit/public/0``
        uses slashes, not dots).
        """
        if (
            not instance
            or "." in instance
            or not worker_id
            or "." in worker_id
        ):
            raise ValueError(
                "md.w subject tokens must be non-empty and contain no '.': "
                f"instance={instance!r} worker_id={worker_id!r}"
            )
        return f"md.w.{instance}.{worker_id}"

    @staticmethod
    def md_worker_pattern() -> str:
        """Every :meth:`md_worker` subject."""
        return "md.w.*.*"

    @staticmethod
    def md_universe(session_id: str) -> str:
        """``md.universe.{session_id}`` — selector changes (§6.4, §8.3)."""
        return f"md.universe.{session_id}"

    @staticmethod
    def md_universe_pattern() -> str:
        """Every :meth:`md_universe` subject."""
        return "md.universe.*"

    @staticmethod
    def md_fetch() -> str:
        """Request-reply subject for market-data queries. One, for everyone.

        Not keyed by anything, unlike :meth:`td_order`. That subject names an
        account because ``serve`` is a competing consumer and only the process
        holding the account may answer for it — an order is owned. A
        read is not: any MD can serve any venue over REST, and the same answer
        comes back whoever produced it. So competing consumers stop being the
        hazard the key exists to avoid and become the point, spreading queries
        across whatever MD processes are up and surviving the loss of any one.

        Where the answer goes is the request's business, not the subject's; see
        ``MdFetchKlines.reply_channel``.
        """
        return "md.fetch"

    @staticmethod
    def md_fetch_reply(caller: str) -> str:
        """Pub/sub channel a caller listens on for its own query results.

        Separate from :meth:`md_session`, which only exists while a strategy
        holds a market-data attach and carries the feeds it subscribed. A
        caller that wants history and no feeds has no such channel, and should
        not have to acquire one to ask a question.
        """
        return f"md.fetch.reply.{caller}"

    @staticmethod
    def td_order(api_id: int) -> str:
        """STS → TD request-reply subject for order entry.

        Per-account on purpose: ``serve`` is a competing consumer, so a shared
        subject would let a TD process that does not hold this account take the
        request. One subject per api_id makes the account's owner the only
        consumer.

        What happens to a request sent while nobody owns the subject is the
        transport's to say, not this name's: Redis parks it until an owner
        arrives, NATS answers at once that there is no responder. A caller that
        sends during a cutover has to be able to live with either, which is why
        STS's rebuild attach retries on a budget.
        """
        return f"td.order.{api_id}"

    @staticmethod
    def td_account_state(api_id: int) -> str:
        """``td.account.state.{api_id}`` (§5.6, §7.1).

        The account worker's availability broadcast. Not :meth:`td_account`,
        which is the request-reply subject for ledger and OMS reads.
        """
        return f"td.account.state.{api_id}"

    @staticmethod
    def td_account_state_pattern() -> str:
        """Every :meth:`td_account_state` subject."""
        return "td.account.state.*"

    @staticmethod
    def procman_report(plane: str, instance: str) -> str:
        """``procman.report.{plane}.{instance}`` (§8.2).

        The Supervisor's liveness report. Not stored. A reader that sees
        the publication stop reclaims nothing (F32).
        """
        if not plane or "." in plane or not instance or "." in instance:
            raise ValueError(
                "procman.report tokens must be non-empty and contain no '.': "
                f"plane={plane!r} instance={instance!r}"
            )
        return f"procman.report.{plane}.{instance}"

    @staticmethod
    def procman_report_pattern() -> str:
        """Every :meth:`procman_report` subject."""
        return "procman.report.*.*"

    @staticmethod
    def td_account(api_id: int) -> str:
        """STS → TD request-reply for account reads that are not order entry.

        Same per-``api_id`` ownership rule as :meth:`td_order`. Kept off the
        order subject so a venue round-trip (e.g. leverage lookup) cannot
        stall submit/cancel acks. Distinct from :meth:`td_ledger`, which is
        a state fan-out key, not a request-reply subject.
        """
        return f"td.account.{api_id}"

    @staticmethod
    def td_backfill(instance: str) -> str:
        """Work queue for re-reading an account's history from its venue.

        Keyed by *instance*, and this is the one unowned job for which that
        matters. The correctness argument for leaving it unkeyed still stands
        in full: a history read is owned by nobody, any TD can ask, the answer
        is the same whoever asked, and the writes are idempotent. None of that
        is about **where the socket opens from**.

        This is the only unowned job that carries a credential.
        ``BackfillSession`` says so outright — "any TD can load the credential
        and ask" — ``mftik_td.backfill.reader`` builds each reader from
        ``row.api_key`` / ``row.api_secret``, and ``backfill_cron`` sweeps every
        account with history on a timer. Unkeyed, a US TD would periodically
        open a venue connection with a JP-only key: the compliance requirement
        failing on a schedule rather than at an edge.

        The old objection to keying does not apply here. It argued that a keyed
        subject parks a request until the account's *owner* takes it, which for
        a retired account is forever — but an instance is up whether or not
        anybody is trading that account, so a retired account is still
        backfilled. And for a jurisdiction-bound credential, "wait until
        ``td-jp-1`` is back" is the correct behaviour rather than a regression.

        Several TD processes on one instance still share this queue, which is
        the competing-consumer spread the original design wanted. What *is*
        owned is the API key's rate-limit budget, fenced with a lock per
        ``api_id`` rather than by the subject — see
        :mod:`mftik_td.backfill.executor`.

        The schedule still sends here (F35). The TD process forwards each
        request to the account worker on ``td.account.{api_id}`` and
        returns that reply. The walk uses the worker's resident pool.
        A subject with no worker falls back to the walk in this process.
        """
        return f"td.backfill.{instance}"

    @staticmethod
    def td_ledger(api_id: int) -> str:
        """TD → STS balance-ledger fan-out (venue balances + TD pre-locks)."""
        return f"td.ledger.{api_id}"

    @staticmethod
    def td_oms(api_id: int) -> str:
        """TD → STS OMS snapshot fan-out for a trading account."""
        return f"td.oms.{api_id}"

    @staticmethod
    def md_feed(topic: str, ticker: UniversalTicker | str) -> str:
        """Logical feed key for attach payloads / refcount (not a subject).

        ``topic.UniversalTicker`` — ``bestquote.Gate_Spot_ETHUSDT``. The topic
        leads because it is the part with a fixed vocabulary, and the ticker is
        one opaque token rather than three fields the reader has to reassemble.

        Kline topics carry their interval (``kline_1m.Gate_Spot_BTCUSDT``); the
        split is on ``.``, so the underscore inside the topic is not a problem.
        """
        return f"{topic}.{ticker}"

    @staticmethod
    def parse_md_feed(feed: str) -> tuple[str, UniversalTicker]:
        """Parse a ``topic.UniversalTicker`` feed key.

        Strict — see :meth:`UniversalTicker.parse`. Feed keys reaching here
        have already been through a boundary that normalized them, and one
        instrument spelled two ways would refcount as two feeds.
        """
        topic, separator, rest = feed.partition(".")
        if not separator or not topic or not rest:
            raise ValueError(
                f"invalid md feed key {feed!r}; expected topic.UniversalTicker, "
                f"e.g. bestquote.Gate_Spot_ETHUSDT"
            )
        return topic, UniversalTicker.parse(rest)

    @staticmethod
    def normalize_md_feed(feed: str) -> str:
        """Accept a human's feed key and return the canonical spelling.

        The lenient counterpart to :meth:`parse_md_feed`, for the boundaries
        that take feed keys from people — strategy YAML, an API query. What it
        returns is what everything downstream should carry.
        """
        topic, separator, rest = feed.partition(".")
        if not separator or not topic or not rest:
            raise ValueError(
                f"invalid md feed key {feed!r}; expected topic.UniversalTicker, "
                f"e.g. bestquote.Gate_Spot_ETHUSDT"
            )
        return Topics.md_feed(topic.strip(), UniversalTicker.resolve(rest))

    @staticmethod
    def private_order(account: str) -> str:
        return f"private.order.{account}"

    @staticmethod
    def private_balance(account: str) -> str:
        return f"private.balance.{account}"
