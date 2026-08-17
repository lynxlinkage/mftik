"""Stream and channel name helpers for the MFT broker protocol."""

from mftik.exchange.tickers import UniversalTicker


class Topics:
    # Request-reply subjects (control plane)
    TD = "td"
    STS = "sts"
    MD = "md"
    SYM = "sym"
    PAPER = "paper"

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
    def sts_td_session(session_id: str) -> str:
        """STS → TD per-session channel (lease heartbeat + cmds)."""
        return f"sts.td.{session_id}"

    @staticmethod
    def sts_md_session(session_id: str) -> str:
        """STS → MD per-session channel (lease heartbeat + subscribe/detach)."""
        return f"sts.md.{session_id}"

    @staticmethod
    def md_session(session_id: str) -> str:
        """MD → STS per-session channel (lease ACK + market data)."""
        return f"md.{session_id}"

    @staticmethod
    def md_fetch() -> str:
        """Request-reply subject for market-data queries. One, for everyone.

        Not keyed by anything, unlike :meth:`td_order`. That subject names an
        account because ``serve`` is a competing-consumer BLPOP and only the
        process holding the account may answer for it — an order is owned. A
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

        Per-account on purpose: ``serve`` is a competing-consumer BLPOP, so a
        shared subject would let a TD process that does not hold this account
        take the request. One subject per api_id makes the account's owner the
        only consumer — and a request sent while nobody owns it waits in the
        list rather than vanishing the way a pub/sub message would.
        """
        return f"td.order.{api_id}"

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
    def td_backfill() -> str:
        """Work queue for re-reading an account's history from its venue.

        Unkeyed, unlike :meth:`td_order`. That subject names an account because
        an order is *owned* — only the process holding the lease may place one.
        A history read is owned by nobody: any TD can load the credential and
        ask, the answer is the same whoever asked, and the writes it produces
        are idempotent. So competing consumers stop being the hazard the key
        exists to avoid and become the point, spreading the work across
        whatever TD processes are up and surviving the loss of any one.

        It also means an account with no live attach still gets backfilled. A
        keyed subject would park the request in a list until somebody attached,
        which for a retired account is forever.

        What *is* owned is the API key's rate-limit budget, and that is fenced
        with a lock per ``api_id`` rather than by the subject — see
        :mod:`mftik_td.backfill.executor`.
        """
        return "td.backfill"

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
        """Logical feed key for attach payloads / refcount (not a Redis subject).

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
