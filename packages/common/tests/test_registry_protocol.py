"""Handshake: refuse a remote whose registry protocol we cannot speak."""

import pytest
from mftik.registry.errors import RegistryError
from mftik.registry.protocol import (
    PROTOCOL,
    PROTOCOL_MIN,
    PROTOCOL_VERSION,
    check_handshake,
    handshake_info,
)


def test_handshake_info_shape() -> None:
    info = handshake_info(mft_version="0.1.0")
    assert info["protocol"] == PROTOCOL
    assert info["protocol_version"] == PROTOCOL_VERSION
    assert info["protocol_min"] == PROTOCOL_MIN
    check_handshake(info)


def test_incompatible_protocol_is_refused() -> None:
    with pytest.raises(RegistryError, match="incompatible"):
        check_handshake(
            {
                "protocol": PROTOCOL,
                "protocol_version": 99,
                "protocol_min": 99,
            }
        )


def test_wrong_protocol_name_is_refused() -> None:
    with pytest.raises(RegistryError, match="not an mft registry"):
        check_handshake(
            {
                "protocol": "other",
                "protocol_version": 1,
                "protocol_min": 1,
            }
        )
