"""Topic strings and BookView's presence. Not a port of the Go tests."""

import enum
import unittest

import mftik
from mftik import topic
from mftik._mftik import topic as native


class TopicTest(unittest.TestCase):
    def test_constants_are_str(self) -> None:
        pairs = {
            "BEST_QUOTE": "bestquote",
            "TRADE": "trade",
            "AGG_TRADE": "aggtrade",
            "ORDER_BOOK_DELTA": "orderbook_delta",
            "TICKER": "ticker",
            "LIQUIDATION": "liquidation",
            "FUNDING_RATE": "funding_rate",
            "OPEN_INTEREST": "open_interest",
            "GREEKS": "greeks",
            "MARK_PRICE": "mark_price",
        }
        for name, value in pairs.items():
            got = getattr(topic, name)
            self.assertEqual(got, value)
            self.assertEqual(type(got), str)
            self.assertNotIsInstance(got, enum.Enum)
            self.assertEqual(getattr(native, name), value)
        self.assertEqual(topic.kline("1m"), "kline_1m")
        self.assertEqual(topic.kline(""), "kline_")
        self.assertEqual(type(topic.kline("4h")), str)
        self.assertEqual(topic.kline("4h"), native.kline("4h"))

    def test_book_view_is_exported(self) -> None:
        self.assertIsInstance(mftik.BookView, type)
        self.assertEqual(mftik.BookView.__module__, "mftik")
