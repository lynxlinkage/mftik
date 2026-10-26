"""Pinned python-build-standalone builds (E04-D073).

The sha256 values are the public checksums from release ``20261021``
``SHA256SUMS`` for ``install_only_stripped`` / ``x86_64-unknown-linux-gnu``.
A download that does not match is discarded.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote

RELEASE = "20261021"
TRIPLE = "x86_64-unknown-linux-gnu"
ASSET = "install_only_stripped"


@dataclass(frozen=True)
class CPythonPin:
    """One linux x86_64 runtime that may be packed."""

    python: str
    patch: str
    sha256: str

    @property
    def filename(self) -> str:
        return f"cpython-{self.patch}+{RELEASE}-{TRIPLE}-{ASSET}.tar.gz"

    @property
    def url(self) -> str:
        # The filename contains ``+``. Encode it so the path is not reread as a space.
        return (
            "https://github.com/astral-sh/python-build-standalone/releases/download/"
            f"{RELEASE}/{quote(self.filename)}"
        )


# Minor version the CLI accepts -> pinned patch and checksum.
CPYTHON: dict[str, CPythonPin] = {
    "3.11": CPythonPin(
        "3.11",
        "3.11.17",
        "224ca8be2d28841e7a382a27c51e74f4ed0aabe249a79294684e782363165948",
    ),
    "3.12": CPythonPin(
        "3.12",
        "3.12.15",
        "6d7521138fd33afddc1ccd08abb007d9a2829e38117c2576f9efdc0ec73ce772",
    ),
    "3.13": CPythonPin(
        "3.13",
        "3.13.17",
        "f829adc9d888e76c8801b4799ed444188d35239fcae3a9808a99ec5cb0d8d92c",
    ),
}

DEFAULT_PYTHON = "3.12"
