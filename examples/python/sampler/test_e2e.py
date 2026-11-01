"""End-to-end test for sampler (E07-D015) and its replay (E07-D024).

The peers are the mftik-testkit shim and nats-server. The timer period
comes from params. The strategy does not subscribe to a feed. The replay
tests record one session with the event log on and run it again in this
process with ``mftik.replay``.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, ClassVar

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))
sys.path.insert(0, str(_HERE))

import mftik  # noqa: E402
from harness import by_hook, drive, logs  # noqa: E402
from sampler import Sampler  # noqa: E402

_TICKER = "Ex_Spot_ABC"
_HOOKS = ("OnInit", "OnStart", "OnTimer", "OnStop")


def _stamp(row: dict[str, object]) -> None:
    now = row["now"]
    if not isinstance(now, int) or now <= 0:
        raise AssertionError(row)
    if row["again"] != now:
        raise AssertionError(row)
    if row["same"] is not True:
        raise AssertionError(row)
    if not isinstance(row["draw"], int):
        raise AssertionError(row)


class SamplerE2E(unittest.TestCase):
    def test_now_is_the_hooks_start_and_rand_is_one_object(self) -> None:
        report = drive(
            {
                "spec": "sampler:Sampler",
                "pythonpath": str(_HERE),
                "params": {"every_s": 0.2},
                "ticker": _TICKER,
                "uid": 7,
                "steps": ["sleep:0.5", "stop"],
            }
        )
        self.assertEqual(report["code"], 0, report["stderr"])
        self.assertEqual(report["orders"], [])
        self.assertNotIn("Traceback", report["stderr"])
        rows = logs(report)
        init = by_hook(rows, "OnInit")
        start = by_hook(rows, "OnStart")
        timer = by_hook(rows, "OnTimer")
        stop = by_hook(rows, "OnStop")
        self.assertEqual(len(init), 1, rows)
        self.assertEqual(len(start), 1, rows)
        self.assertGreaterEqual(len(timer), 1, rows)
        self.assertEqual(len(stop), 1, rows)
        for row in (*init, *start, *timer, *stop):
            _stamp(row)
        self.assertLessEqual(init[0]["now"], start[0]["now"])
        self.assertLessEqual(start[0]["now"], timer[0]["now"])
        self.assertLessEqual(timer[-1]["now"], stop[0]["now"])


class Draws(Sampler):
    """Sampler that keeps each hook's ``now`` and draw instead of logging them.

    It draws from the same ``random.Random`` in the same order as Sampler.
    """

    seen: ClassVar[list[tuple[str, int, int]]] = []

    def _stamp(self, ctx: mftik.Context, hook: str) -> None:
        rng = self._rng
        if rng is None or ctx.rand() is not rng:
            raise RuntimeError("sampler: rand is not the session's")
        if ctx.now() != ctx.now():
            raise RuntimeError("sampler: now moved inside a hook")
        type(self).seen.append((hook, ctx.now(), rng.getrandbits(64)))


class Changed(Sampler):
    """Sampler whose timer raises: a modified strategy (E07-D024)."""

    def _on_timer(self, ctx: mftik.Context) -> None:
        raise ValueError("sampler: changed")


class SamplerReplay(unittest.TestCase):
    """Record once, then replay the log in this process (E07-D024)."""

    work: ClassVar[Path]
    log_dir: ClassVar[Path]
    rows: ClassVar[list[dict[str, Any]]]

    @classmethod
    def setUpClass(cls) -> None:
        cls.work = Path(tempfile.mkdtemp(prefix="mftik-sampler-replay-"))
        cls.log_dir = cls.work / "eventlog"
        report = drive(
            {
                "spec": "sampler:Sampler",
                "pythonpath": str(_HERE),
                "params": {"every_s": 0.2},
                "ticker": _TICKER,
                "uid": 7,
                "steps": ["sleep:0.7", "stop"],
                "event_log_dir": str(cls.log_dir),
            }
        )
        if report["code"] != 0:
            raise AssertionError(report["stderr"])
        cls.rows = [row for row in logs(report) if row.get("hook") in _HOOKS]

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.work, ignore_errors=True)

    def test_the_recorded_session_replays_with_no_mismatch(self) -> None:
        self.assertTrue(any(self.log_dir.glob("*.mel")), list(self.log_dir.iterdir()))
        report = mftik.replay(Sampler, self.log_dir)
        self.assertEqual(report.mismatches, ())
        self.assertEqual(report.skipped, 0, report)
        self.assertEqual(report.missing, 0, report)
        self.assertEqual(report.orders, 0, report)
        self.assertEqual(report.reads, 0, report)
        self.assertEqual(report.inputs, len(self.rows), (report, self.rows))
        self.assertGreaterEqual(len(by_hook(self.rows, "OnTimer")), 2, self.rows)
        self.assertEqual(report.sdk_version, mftik.__version__)
        self.assertEqual(report.incarnation, 1)
        self.assertNotEqual(report.session_id, "")
        self.assertIs(type(report.mismatches), tuple)
        with self.assertRaises(AttributeError):
            report.inputs = 0  # type: ignore[misc]

    def test_now_and_rand_are_the_recorded_ones_on_every_replay(self) -> None:
        recorded = [(row["hook"], row["now"], row["draw"]) for row in self.rows]
        for _ in range(2):
            Draws.seen = []
            report = mftik.replay(Draws, str(self.log_dir), incarnation=1)
            self.assertEqual(report.mismatches, ())
            self.assertEqual(Draws.seen, recorded)

    def test_a_changed_strategy_mismatches(self) -> None:
        report = mftik.replay(Changed, self.log_dir)
        self.assertNotEqual(report.mismatches, ())
        ended = [line for line in report.mismatches if "ValueError: sampler: changed" in line]
        self.assertEqual(len(ended), 1, report.mismatches)
        self.assertIn("recorded normally", ended[0])
        self.assertTrue(
            any("the replay did not make it" in line for line in report.mismatches),
            report.mismatches,
        )
        again = mftik.replay(Sampler, self.log_dir)
        self.assertEqual(again.mismatches, ())


if __name__ == "__main__":
    unittest.main()
