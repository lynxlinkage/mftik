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


def test_handshake_info_shape(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    info = handshake_info(mftik_version="0.1.0")
    assert info["protocol"] == PROTOCOL
    assert info["protocol_version"] == PROTOCOL_VERSION
    assert info["protocol_min"] == PROTOCOL_MIN
    assert info["extras"] == {}
    assert info["env_generation"] == 0
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


def test_handshake_info_advertises_matching_extras(tmp_path, monkeypatch) -> None:
    from mftik.envapply import ApplySpec, apply_packages
    from mftik.environment import NodeEnv

    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))

    def plant(dest, packages):  # noqa: ANN001
        for name in packages:
            pkg = dest / name
            pkg.mkdir()
            (pkg / "__init__.py").write_text("ok\n")

    apply_packages(
        NodeEnv(tmp_path),
        {"numpy": ApplySpec(version="2.2.1", dist="numpy")},
        installer=plant,
    )
    info = handshake_info(data_dir=tmp_path)
    assert info["env_generation"] == 1
    assert info["extras"] == {"numpy": {"version": "2.2.1", "dist": "numpy"}}
    assert "source" not in info["extras"]["numpy"]
    check_handshake(info)


def test_check_remote_extras_is_names_only() -> None:
    from mftik.registry.protocol import check_remote_extras, extra_names

    info = {
        "protocol": PROTOCOL,
        "extras": {"numpy": {"version": "2.2.1", "dist": "numpy"}},
    }
    assert extra_names(info) == frozenset({"numpy"})
    check_remote_extras(info, frozenset({"numpy"}))
    check_remote_extras({"extras": {"numpy": "1.0"}}, frozenset({"numpy"}))
    with pytest.raises(RegistryError, match="numpy"):
        check_remote_extras(info, frozenset())


def test_wrong_protocol_name_is_refused() -> None:
    with pytest.raises(RegistryError, match="not an mftik registry"):
        check_handshake(
            {
                "protocol": "other",
                "protocol_version": 1,
                "protocol_min": 1,
            }
        )
