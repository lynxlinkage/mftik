"""Stream and channel name helpers for the MFT broker protocol."""


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
    def td_oms(api_id: int) -> str:
        """TD → STS OMS snapshot fan-out for a trading account."""
        return f"td.oms.{api_id}"

    @staticmethod
    def md_feed(venue: str, topic: str, symbol: str) -> str:
        """Logical feed key for attach payloads / refcount (not a Redis subject)."""
        return f"{venue}.{topic}.{symbol}"

    @staticmethod
    def parse_md_feed(feed: str) -> tuple[str, str, str]:
        """Parse ``venue.topic.symbol`` feed key into components."""
        parts = feed.split(".", 2)
        if len(parts) != 3 or not all(parts):
            raise ValueError(
                f"invalid md feed key {feed!r}; expected venue.topic.symbol"
            )
        return parts[0], parts[1], parts[2]

    @staticmethod
    def private_order(account: str) -> str:
        return f"private.order.{account}"

    @staticmethod
    def private_balance(account: str) -> str:
        return f"private.balance.{account}"
