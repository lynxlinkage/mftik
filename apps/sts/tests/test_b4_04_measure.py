"""B4-04: session no-responders, plus a short hop and GIL smoke.

The bounds are sanity checks. The numbers are printed; §5.3 records
the long run from ``scripts/b4_04_measure.py``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest
from mftik.protocol.reject_codes import RejectCode


def _measure() -> ModuleType:
    path = Path(__file__).resolve().parents[3] / "scripts" / "b4_04_measure.py"
    spec = importlib.util.spec_from_file_location("b4_04_measure_sts", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def measure() -> ModuleType:
    return _measure()


@pytest.mark.integration
async def test_submit_without_td_waits_out_the_ack(
    measure: ModuleType, tmp_path: Path
) -> None:
    result = await measure.session_no_responders(tmp_path)
    print(result)
    leaked = [
        name
        for name in sys.modules
        if name.startswith("_mftik_reg_private_B404")
    ]
    assert leaked == []
    assert result["exit_code"] == 0
    assert result["accepted"] is False
    assert result["code"] == int(RejectCode.TD_NO_ACK)
    assert result["reason"] == "no ack from TD"
    assert result["error"] == ""
    # The 503 does not arrive, so this is the 2s ack timeout, not a
    # fast failure and not the broker's default request timeout.
    assert 1.6 < result["elapsed_s"] < 3.0


@pytest.mark.integration
async def test_ack_hop_smoke(measure: ModuleType) -> None:
    spin_s = 0.1
    hops = await measure.measure_hops(
        n_idle=20,
        n_md=20,
        n_spin=8,
        spin_s=spin_s,
        md_target_per_s=2000,
        warmup=2,
    )
    print(measure._report({"hops": hops}))
    for name in ("idle", "md"):
        row = hops[name]
        assert row["timeouts"] == 0, row["errors"]
        assert row["hop"]["n"] == 20
        assert row["hop"]["p99"] < 0.1
    hooked = hops["cpu_hook"]
    assert hooked["timeouts"] == 0, hooked["errors"]
    assert hooked["hop"]["n"] == 8
    assert hooked["wall"]["p50"] > spin_s * 0.8
    assert hooked["hop"]["p99"] < spin_s + 0.25
    assert hooked["hook_hold"]["p99"] < 0.05


@pytest.mark.integration
async def test_gil_switch_interval_smoke(measure: ModuleType) -> None:
    gil = await measure.measure_gil(
        seconds=0.8,
        intervals=(0.005, 0.001, 0.0005),
        ping_every_s=0.05,
        ack_delay_s=0.2,
    )
    print(measure._report({"gil": gil}))
    assert len(gil["intervals"]) == 3
    for row in gil["intervals"]:
        assert row["ack_ok"], row["ack_error"]
        assert row["ack_read_during_spin"] is True
        assert row["heartbeat_samples"] >= 1
        assert row["heartbeat_overrun"]["max"] < 0.5
        assert row["inbox_delay"]["n"] >= 1
        assert row["inbox_delay"]["max"] < 0.25
        assert row["hb_at_risk"] is False
        assert row["ack_at_risk"] is False
