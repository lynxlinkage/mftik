"""B4-04: a 503 stays on the connection that published.

nats-server writes no-responders only onto a subscription owned by the
publishing connection. These tests lock that on the server CI runs
(``nats:2.11-alpine``, ``-m 8222``, no config) and on the kwargs
``NatsTransport.connect`` actually uses. A nats-py or server upgrade
that starts delivering the 503 to the other connection fails here.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


def _measure() -> ModuleType:
    path = Path(__file__).resolve().parents[3] / "scripts" / "b4_04_measure.py"
    spec = importlib.util.spec_from_file_location("b4_04_measure_raw", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def measure() -> ModuleType:
    return _measure()


@pytest.mark.integration
def test_no_responders_503_stays_on_the_publishing_connection(
    measure: ModuleType,
) -> None:
    result = measure.probe_no_responders()
    has = measure._has_503
    print(
        "server",
        result["server_info_version"],
        "headers",
        result["server_info_headers"],
        "nr_without_headers",
        result["no_responders_without_headers"],
    )
    version = result["server_info_version"]
    assert isinstance(version, str) and version.startswith("2.")
    assert result["server_info_headers"] is True
    assert has(result["publisher_subscribed"]["publisher"])
    assert not has(result["publisher_subscribed"]["other"])
    assert not has(result["other_only"]["publisher"])
    assert not has(result["other_only"]["other"])
    assert has(result["both_subscribed"]["publisher"])
    assert not has(result["both_subscribed"]["other"])
    plain = result["publisher_headers_without_no_responders"]
    assert not has(plain["publisher"])
    assert not has(plain["other"])
    bare = result["other_without_headers"]
    assert not has(bare["publisher"])
    assert not has(bare["other"])
    assert "headers" in result["no_responders_without_headers"]


@pytest.mark.integration
async def test_product_publish_with_reply_503_stays_on_the_sender(
    measure: ModuleType,
) -> None:
    result = await measure.probe_product_connections()
    print(
        "cross",
        result["cross_received"],
        "same_header",
        result["same_header"],
        "same_data",
        result["same_data"],
        "elapsed",
        f"{result['same_elapsed_s']:.4f}",
    )
    assert result["cross_received"] == []
    assert result["same_header"] == {"Status": "503"}
    assert result["same_data"] == b""
    assert result["same_elapsed_s"] < 0.2
