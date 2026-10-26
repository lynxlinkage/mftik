"""Strategy volume files (PROTOCOL §9.1.4, E04-D044).

``write`` replaces one file atomically. ``read`` tries the volume directory
first, then the input directory. An empty directory string is skipped, not
the process's current directory. A ``str`` passed as ``data`` is not encoded.
"""

from __future__ import annotations

import os
from typing import Any

from mftik._mftik import volume_read, volume_write

__all__ = ["read", "write"]

_STR_DATA = "volume data must be bytes-like, not str"


def _directory(path: str | os.PathLike[str]) -> str:
    text = os.fspath(path)
    if isinstance(text, bytes):
        raise TypeError("volume path must be a str, not bytes")
    return text


def write(dir: str | os.PathLike[str], name: str, data: bytes | bytearray | memoryview) -> None:
    """Replace ``dir``/``name`` with ``data``. ``str`` raises ``TypeError``."""
    if isinstance(data, str):
        raise TypeError(_STR_DATA)
    volume_write(_directory(dir), name, data)


def read(
    volume_dir: str | os.PathLike[str], input_dir: str | os.PathLike[str], name: str
) -> bytes | None:
    """The bytes of ``name``, or ``None`` when neither directory has it."""
    found: Any = volume_read(_directory(volume_dir), _directory(input_dir), name)
    if found is None:
        return None
    if isinstance(found, bytes):
        return found
    return bytes(found)
