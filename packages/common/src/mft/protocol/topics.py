"""Stream and channel name helpers for the MFT broker protocol."""


class Topics:
    # Request-reply subjects (control plane)
    TD = "td"
    STS = "sts"
    MD = "md"

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
    def sts_session(session_id: str) -> str:
        """STS → TD per-session channel (lease heartbeat + cmds)."""
        return f"sts.{session_id}"

    @staticmethod
    def td_oms(api_id: int) -> str:
        """TD → STS OMS snapshot fan-out for a trading account."""
        return f"td.oms.{api_id}"

    @staticmethod
    def md_ticker(exchange: str, symbol: str) -> str:
        return f"md.ticker.{exchange}.{symbol}"

    @staticmethod
    def md_kline(exchange: str, symbol: str, interval: str) -> str:
        return f"md.kline.{exchange}.{symbol}.{interval}"

    @staticmethod
    def private_order(account: str) -> str:
        return f"private.order.{account}"

    @staticmethod
    def private_balance(account: str) -> str:
        return f"private.balance.{account}"
