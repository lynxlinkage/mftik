"""Handshake: refuse a remote whose registry protocol we cannot speak."""

import pytest
from mftik.registry.errors import MissingRemoteExtras, RegistryError
from mftik.registry.protocol import (
    PROTOCOL,
    PROTOCOL_MIN,
    PROTOCOL_VERSION,
    check_handshake,
    check_remote_extras,
    extra_names,
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
    names_only = handshake_info(data_dir=tmp_path, pins=False)
    assert names_only["extras"] == {"numpy": {}}
    assert extra_names(names_only) == frozenset({"numpy"})
    check_remote_extras(names_only, frozenset({"numpy"}))


def test_check_remote_extras_is_names_only() -> None:
    info = {
        "protocol": PROTOCOL,
        "extras": {"numpy": {"version": "2.2.1", "dist": "numpy"}},
    }
    assert extra_names(info) == frozenset({"numpy"})
    check_remote_extras(info, frozenset({"numpy"}))
    check_remote_extras({"extras": {"numpy": "1.0"}}, frozenset({"numpy"}))
    check_remote_extras({"extras": {"numpy": {}}}, frozenset({"numpy"}))
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


def test_a_missing_extra_refusal_carries_the_names_as_data() -> None:
    """The message is for a person; the names are for the client.

    A UI that offered "import these" by regexing the sentence broke the first
    time the sentence learned to mention versions, and rendered fragments of
    English as package names.
    """
    info = {
        "protocol": "mftik.registry",
        "protocol_version": 1,
        "protocol_min": 1,
        "extras": {"numpy": {"version": "2.0", "dist": "numpy"}, "torch": {}},
    }
    with pytest.raises(MissingRemoteExtras) as caught:
        check_remote_extras(info, frozenset({"pandas"}), {"numpy": "1.26.4"})
    exc = caught.value
    assert exc.missing == ("numpy", "torch")
    assert exc.rows() == [
        {"name": "numpy", "version": "1.26.4"},
        {"name": "torch", "version": None},
    ]
    # Still a RegistryError, so every existing handler keeps working.
    assert isinstance(exc, RegistryError)
    assert "1.26.4" in str(exc)


def test_no_refusal_when_the_names_are_covered() -> None:
    info = {
        "protocol": "mftik.registry",
        "protocol_version": 1,
        "protocol_min": 1,
        "extras": {"numpy": {"version": "9.9", "dist": "numpy"}},
    }
    # A version difference is not a connect error.
    check_remote_extras(info, frozenset({"numpy"}))
