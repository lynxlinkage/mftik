"""End-to-end test for md_watch (E05-D031).

The peers are the mftik-testkit shim, nats-server, and fake SYM and MD
responders. The ticker and the timer come from params.
"""

from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import by_hook, drive, logs  # noqa: E402

_HERE = Path(__file__).resolve().parent
_TICKER = "Ex_Spot_ABC"


def _close(value: object, want: float) -> bool:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return False
    return math.isclose(value, want)


class MdWatchE2E(unittest.TestCase):
    def test_trade_book_and_timer(self) -> None:
        report = drive(
            {
                "spec": "md_watch:MdWatch",
                "pythonpath": str(_HERE),
                "params": {"ticker": _TICKER, "every_s": 0.2},
                "ticker": _TICKER,
                "uid": 7,
                "feeds": ["trade", "book"],
                "steps": ["trade", "book", "sleep:1", "stop"],
            }
        )
        self.assertEqual(report["code"], 0, report["stderr"])
        self.assertEqual(report["orders"], [])
        rows = logs(report)
        init = by_hook(rows, "OnInit")
        self.assertEqual(len(init), 1, rows)
        self.assertEqual(init[0]["ticker"], _TICKER)
        self.assertEqual(init[0]["uid"], 7)

        trade = by_hook(rows, "OnTrade")
        self.assertEqual(len(trade), 1, rows)
        self.assertEqual(trade[0]["trade_id"], "t1")
        self.assertEqual(trade[0]["uid"], 7)
        self.assertTrue(_close(trade[0]["price"], 11.0), trade[0])
        self.assertTrue(_close(trade[0]["qty"], 2.0), trade[0])

        book = by_hook(rows, "OnBook")
        self.assertEqual(len(book), 1, rows)
        self.assertIs(book[0]["synced"], True)
        self.assertEqual(book[0]["uid"], 7)
        self.assertTrue(_close(book[0]["bid_qty"], 2.0), book[0])
        self.assertTrue(_close(book[0]["ask_qty"], 1.0), book[0])
        self.assertTrue(_close(book[0]["bid"], 11.0), book[0])
        self.assertTrue(_close(book[0]["ask"], 12.0), book[0])
        self.assertIsInstance(book[0]["bid_tick"], int)
        self.assertIsInstance(book[0]["ask_tick"], int)

        self.assertGreaterEqual(len(by_hook(rows, "OnTimer")), 1, rows)
        self.assertEqual(len(by_hook(rows, "OnStop")), 1, rows)
        self.assertNotIn("Traceback", report["stderr"])


if __name__ == "__main__":
    unittest.main()
