"""End-to-end test for the Rust md_watch (E08-D006, E08-D009).

The harness, the scenario, and the checks are those of
``examples/python/md_watch/test_e2e.py``, with the md_watch binary as the
worker. The test also reads the ``counts`` file it leaves in the volume.

``MFTIK_MD_WATCH_ROOTFS`` is the rootfs of an unpacked ``mftiko-pack -bin``
image: the worker is its ``bin/strategy``, started from the rootfs. Without
it, the test builds ``target/debug/md_watch`` with cargo.
"""

from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

from harness import by_hook, drive, logs  # noqa: E402

_ROOT = Path(__file__).resolve().parents[3]
_TICKER = "Ex_Spot_ABC"


def _worker() -> dict[str, str]:
    rootfs = os.environ.get("MFTIK_MD_WATCH_ROOTFS", "")
    if rootfs:
        root = Path(rootfs).resolve()
        return {"program": str(root / "bin" / "strategy"), "cwd": str(root)}
    subprocess.run(["cargo", "build", "-p", "mftik-md-watch"], cwd=_ROOT, check=True)
    return {"program": str(_ROOT / "target" / "debug" / "md_watch")}


def _close(value: object, want: float) -> bool:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return False
    return math.isclose(value, want)


def _counts(report: dict[str, Any]) -> object:
    text = report["volume"]
    if not isinstance(text, str):
        raise AssertionError(repr(text))
    return json.loads(text)


class MdWatchE2E(unittest.TestCase):
    worker: dict[str, str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.worker = _worker()

    def _scenario(self, **extra: Any) -> dict[str, Any]:
        body: dict[str, Any] = {
            **self.worker,
            "params": {"ticker": _TICKER, "every_s": 0.2},
            "ticker": _TICKER,
            "uid": 7,
            "feeds": ["trade", "book"],
            "volume_name": "counts",
            "steps": ["trade", "book", "sleep:1", "stop"],
        }
        body.update(extra)
        return body

    def test_trade_book_and_timer(self) -> None:
        report = drive(self._scenario())
        self.assertEqual(report["code"], 0, report["stderr"])
        self.assertEqual(report["orders"], [])
        rows = logs(report)
        init = by_hook(rows, "OnInit")
        self.assertEqual(len(init), 1, rows)
        self.assertEqual(init[0]["ticker"], _TICKER)
        self.assertEqual(init[0]["uid"], 7)
        self.assertEqual((init[0]["trades"], init[0]["books"]), (0, 0))

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
        stop = by_hook(rows, "OnStop")
        self.assertEqual(len(stop), 1, rows)
        self.assertEqual((stop[0]["trades"], stop[0]["books"]), (1, 1))
        self.assertEqual(_counts(report), {"trades": 1, "books": 1})
        self.assertNotIn("panicked", report["stderr"])

    def test_counts_continue_from_the_volume(self) -> None:
        report = drive(
            self._scenario(
                params={"ticker": _TICKER, "every_s": 30},
                volume_data='{"trades":4,"books":2}',
                # Stop drops MD that is still on the ring. Wait for OnTrade.
                steps=["trade", "sleep:1", "stop"],
            )
        )
        self.assertEqual(report["code"], 0, report["stderr"])
        rows = logs(report)
        init = by_hook(rows, "OnInit")
        self.assertEqual(len(init), 1, rows)
        self.assertEqual((init[0]["trades"], init[0]["books"]), (4, 2))
        self.assertEqual(len(by_hook(rows, "OnTrade")), 1, rows)
        self.assertEqual(by_hook(rows, "OnTimer"), [])
        stop = by_hook(rows, "OnStop")
        self.assertEqual(len(stop), 1, rows)
        self.assertEqual((stop[0]["trades"], stop[0]["books"]), (5, 2))
        self.assertEqual(_counts(report), {"trades": 5, "books": 2})


if __name__ == "__main__":
    unittest.main()
