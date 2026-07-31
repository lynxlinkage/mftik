"""Stream and channel name helpers for the MFT broker protocol."""


class Topics:
    # Point-to-point streams (Redis Streams)
    CMD_TRADING = "cmd.trading"
    CMD_STRATEGY = "cmd.strategy"
    CMD_ADAPTER = "cmd.adapter"
    CMD_MARKET_DATA = "cmd.market_data"

    # Heartbeats / control
    HEARTBEAT = "sys.heartbeat"

    @staticmethod
    def log_session(session_id: str) -> str:
        """Pub/sub channel for live session logs."""
        return f"log.session.{session_id}"

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
