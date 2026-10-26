"""SDK and proto types. Not a port of the Go tests."""

import math
import unittest

import mftik
from mftik.common.v1 import Side
from mftik.md.v1 import BestQuote, BookSnapshot, FeedEvent, Kline, Klines, TapePage
from mftik.td.v1 import Balance, Fill, Order, Position, Recon


class TypesTest(unittest.TestCase):
    def test_import_does_not_load_protobuf(self) -> None:
        import sys

        self.assertFalse(any(name.startswith("google.protobuf") for name in sys.modules))

    def test_hook_messages_are_present(self) -> None:
        for name in (
            "BestQuote",
            "Trade",
            "AggTrade",
            "Kline",
            "Ticker",
            "Liquidation",
            "FundingRate",
            "OpenInterest",
            "Greeks",
            "MarkPrice",
            "OrderBookDelta",
            "TapePage",
            "BookSnapshot",
            "Klines",
            "Level",
            "Meta",
            "FeedEvent",
            "TapeEvent",
            "Cursor",
            "Gap",
        ):
            self.assertTrue(hasattr(mftik.md.v1, name), name)
        for name in ("Order", "Position", "Balance", "Fill", "Recon"):
            self.assertTrue(hasattr(mftik.td.v1, name), name)
        for name in ("Side", "OrderType", "TimeInForce"):
            self.assertTrue(hasattr(mftik.common.v1, name), name)
        for name in ("OrderStatus", "OrderCause", "ReconReason"):
            self.assertTrue(hasattr(mftik.td.v1, name), name)

    def test_unknown_enum_keeps_the_int_and_rejects_bool(self) -> None:
        side = Side(99)
        self.assertEqual(int(side), 99)
        self.assertIsInstance(side, Side)
        self.assertEqual(Side.SIDE_BUY, 1)
        with self.assertRaises(ValueError):
            Side(True)

    def test_order_keywords_drop_nan_and_copy_params(self) -> None:
        order = mftik.Order(side=99, qty=float("nan"), price=1.0, params={"a": "b"})
        self.assertIsNone(order.qty)
        self.assertEqual(order.price, 1.0)
        self.assertEqual(order.side, 99)
        order.params["a"] = "c"
        self.assertEqual(order.params["a"], "b")
        with self.assertRaises(TypeError):
            mftik.Order(1)
        with self.assertRaises(TypeError):
            mftik.Order(side=True)
        with self.assertRaises(AttributeError):
            order.uid = 2

    def test_info_and_tape_query(self) -> None:
        ticker = mftik.UniversalTicker(venue="Bybit", category="Perp", symbol="BTCUSDT")
        self.assertEqual(str(ticker), "Bybit_Perp_BTCUSDT")
        info = mftik.Info(
            ticker=ticker,
            status="TRADABLE",
            filters={"price_tick": 0.5},
            base="BTC",
            quote="USDT",
            contract_value_ccy="BTC",
            price_ccy="USDT",
            settle="USDT",
        )
        self.assertEqual(info.status, "TRADABLE")
        self.assertTrue(info.is_tradable())
        self.assertEqual(info.price_tick(), 0.5)
        self.assertTrue(info.linear())
        self.assertTrue(info.u_margined())
        info.filters["price_tick"] = 9.0
        self.assertEqual(info.filters["price_tick"], 0.5)
        missing = mftik.Info(ticker=ticker)
        self.assertEqual(missing.price_tick(), 0.0)
        self.assertFalse(missing.is_tradable())
        unset = mftik.Info(ticker=ticker, filters={"price_tick": float("nan")})
        self.assertTrue(math.isnan(unset.price_tick()))
        query = mftik.TapeQuery(limit=1, since_ns=4)
        self.assertIsNone(query.before)
        self.assertEqual(query.limit, 1)

    def test_error_hierarchy_matches_the_go_text(self) -> None:
        cases = (
            (mftik.NotInInit, "sdk: only OnInit and OnStart may call this"),
            (mftik.UnknownUid, "sdk: the instrument is not known to this session"),
            (mftik.UnknownAccount, "sdk: the account is not bound to this session"),
            (mftik.AlgoNotDeclared, "sdk: the algo_id is not declared for this account"),
            (mftik.NoFeed, "sdk: the session has no such feed"),
            (mftik.Stopping, "sdk: the session is stopping"),
            (mftik.Error, "sdk: the td path is not wired yet"),
            (mftik.Error, "a"),
            (mftik.ReadError, "mdpath: md.view: context deadline exceeded"),
            (mftik.ReadError, "mdpath: md.fetch: nats: no responders available for request"),
        )
        for cls, text in cases:
            err = cls(text)
            self.assertIsInstance(err, mftik.Error)
            self.assertEqual(str(err), text)
        self.assertTrue(issubclass(mftik.ContextError, mftik.Error))
        self.assertFalse(isinstance(mftik.Error("a"), mftik.NotInInit))

    def test_proto_messages_are_not_constructed_from_python(self) -> None:
        for cls in (
            BestQuote,
            BookSnapshot,
            FeedEvent,
            Kline,
            Klines,
            TapePage,
            Order,
            Position,
            Balance,
            Fill,
            Recon,
        ):
            with self.assertRaises(TypeError):
                cls()

    def test_origin_values_match_go(self) -> None:
        self.assertEqual(mftik.Origin.from_td, 1)
        self.assertEqual(mftik.Origin.local_reject, 2)
        self.assertEqual(mftik.Origin.timeout, 3)
        self.assertEqual(mftik.RejectedRequest.cancel, 1)
        self.assertEqual(mftik.RejectedRequest.batch_cancel, 2)
        with self.assertRaises(ValueError):
            mftik.Origin(99)


if __name__ == "__main__":
    unittest.main()
